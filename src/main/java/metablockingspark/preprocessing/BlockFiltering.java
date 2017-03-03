/*
 * Copyright 2017 vefthym.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metablockingspark.preprocessing;

import com.google.common.collect.Ordering;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class BlockFiltering {
    
    static final Logger logger = Logger.getLogger(BlockFiltering.class.getName());
    
    SparkSession spark;
    String blockSizesOutputPath, entityIndexOutputPath;

    public BlockFiltering(SparkSession spark, String blockSizesOutputPath, String entityIndexOutputPath) {
        this.spark = spark;
        this.blockSizesOutputPath = blockSizesOutputPath;
        this.entityIndexOutputPath = entityIndexOutputPath;
    }

    //resulting key:blockID, value:entityIds array                            
    private JavaPairRDD<Integer,Integer[]> parseBlockCollection(JavaRDD<String> blockingInput) {
        System.out.println("Parsing the blocking collection...");
        return blockingInput
            .map(line -> line.split("\t")) //split to [blockId, [entityIds]]
            .filter(line -> line.length == 2) //only keep lines of this format
            .mapToPair(pair -> {                
                int blockId = Integer.parseInt(pair[0]);
                String[] entities = pair[1].replaceFirst(";", "").split("#");
                if (entities == null || entities.length == 0) {
                    return null;
                }
                List<Integer> outputEntities = new ArrayList<>(); //possible (but not really probable) cause of OOM (memory errors) if huge blocks exist
                for (String entity : entities) {
                    if (entity.isEmpty()) continue; //in case the last entityId finishes with '#'
                    Integer entityId = Integer.parseInt(entity);			                    
                    outputEntities.add(entityId);
                }
                return new Tuple2<Integer, Integer[]>(blockId, outputEntities.toArray(new Integer[outputEntities.size()]));
            })
            .filter(x -> x != null);
    }
    
    //input: a JavaPairRDD of key:blockID, value:entityIds array        
    //outpu: a JavaPairRDD of key:inverseUtility (=max of D1 entities, D2 entities in this block), value:blockId (=input key)
    public JavaPairRDD<Integer,Integer> getBlockSizes(JavaPairRDD<Integer,Integer[]> parsedBlocks) {
        System.out.println("Getting the block sizes...");
        //LongAccumulator numComparisons = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
        return parsedBlocks         
            .mapToPair(pair -> {
                int blockId = pair._1();
                Integer[] entities = pair._2();
                
                int numEntities = entities.length;
                int D1counter = 0;
                for (int entity : entities) {                                        
                    if (entity >= 0) {
                        D1counter++;
                    } 
                }
                int D2counter = numEntities-D1counter;
                long blockComparisons = D1counter * D2counter;
                //numComparisons.add(blockComparisons);
                
                int inverseUtility = Math.max(D1counter, D2counter);
                if (blockComparisons == 0) {
                    inverseUtility = 0;
                }                
                Tuple2<Integer,Integer> result = new Tuple2<>(inverseUtility, blockId);                
                return result;
            })
            .filter (x -> x != null)
            .sortByKey(false, 1); //save in descending utility order //TODO: check numPartitions      
    }
        
    private JavaPairRDD<Integer, Integer[]> getEntityIndex(JavaPairRDD<Integer,Integer[]> parsedBlocks, JavaPairRDD<Integer,Integer> blockSizes, LongAccumulator BLOCK_ASSIGNMENTS) {
        System.out.println("Creating the entity index...");
        //get pairs of the form (entityId, blockId)
        JavaPairRDD<Integer, Integer> entityIndexMapperResult = parsedBlocks
            .flatMapToPair(pair -> {
                int blockId = pair._1();
                Integer[] entities = pair._2();
                
                List<Tuple2<Integer,Integer>> mapResults = new ArrayList<>(); //possible (but not really probable) cause of OOM (memory errors) if huge blocks exist
                for (int entity : entities) {                    
                    mapResults.add(new Tuple2<>(entity, blockId));
                }
                return mapResults.iterator();
            }); //end of EntityIndexMapper logic 

        //add an integer rank to each blockId, starting with 0 (blocks are sorted in descending utility)
        Map<Integer,Long> blocksRanking = blockSizes.values().zipWithIndex().collectAsMap(); 
//        JavaPairRDD<Integer,Long> blocksRankingRDD = blockSizes.values().zipWithIndex(); 
        
        //create the entity index (similar to EntityIndexReducer)
        return entityIndexMapperResult
                .mapValues(x -> blocksRanking.get(x)) //replace all block Ids with their rank in the sorted list of blocks (by descending utility)
//                .mapValues(x -> blocksRankingRDD.lookup(x).get(0)) //alternative
                .groupByKey()                
                .mapValues(iter -> Ordering.natural().sortedCopy(iter)) //order the blocks of each entity by their rank (highest utility first)
                .mapToPair(pair -> {
                    final int MAX_BLOCKS = ((Double)Math.floor(3*pair._2().size()/4+1)).intValue(); //|_ 3|Bi|/4+1 _| //preprocessing
                    List<Integer> blocksKept = new ArrayList<>(); //ranking is needed here (blocks are already sorted and arraylist keeps insertion order)
                    int indexedBlocks = 0;
                    for (long blockId : pair._2()) {
                        blocksKept.add((int)blockId);
                        if (++indexedBlocks == MAX_BLOCKS) { break;} //comment-out this line to skip block filtering
                    }
                    Integer[] entityIndex = new Integer[blocksKept.size()];
                    entityIndex = blocksKept.toArray(entityIndex);
                    BLOCK_ASSIGNMENTS.add(entityIndex.length);
                    return new Tuple2<Integer,Integer[]>(pair._1(), entityIndex); //the entity index for the current entity
                });                
                //.saveAsObjectFile(entityIndexOutputPath);
//                .saveAsTextFile(entityIndexOutputPath);
                //.saveAsNewAPIHadoopFile(entityIndexOutputPath, VIntWritable.class, VIntArrayWritable.class, SequenceFileOutputFormat.class);                
    }
    
        
    public JavaPairRDD<Integer, Integer[]> run(JavaRDD<String> blockingInput, LongAccumulator BLOCK_ASSIGNMENTS) {        
        JavaPairRDD<Integer,Integer[]> parsedBlocks = parseBlockCollection(blockingInput);
        parsedBlocks.persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        JavaPairRDD<Integer,Integer> blockSizes = getBlockSizes(parsedBlocks);
        blockSizes.cache();        
//        blockSizes
//                .map(x -> x._1()+","+ x._2())         //to remove the parentheses
//                .saveAsTextFile(blockSizesOutputPath);     
        return getEntityIndex(parsedBlocks, blockSizes, BLOCK_ASSIGNMENTS);
    }
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputPath;
        String blockSizesOutputPath;
        String entityIndexOutputPath;
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:/temp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testInput";
            blockSizesOutputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testOutputBlockSizes";
            entityIndexOutputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testOutputEntityIndex";
        } else {            
            tmpPath = "/file:/tmp";
            master = "spark://master:7077";
            inputPath = args[0];
            blockSizesOutputPath = args[1];
            entityIndexOutputPath = args[2];            
            
            // delete existing output directories
            try {                
                Utils.deleteHDFSPath(blockSizesOutputPath);
                Utils.deleteHDFSPath(entityIndexOutputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(BlockFiltering.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        System.out.println("\n\nStarting BlockFiltering, reading from "+inputPath+
                " and writing block sizes to "+blockSizesOutputPath+
                " and entity index to "+entityIndexOutputPath);
        
        SparkSession spark = SparkSession.builder()
            .appName("BlockFiltering")
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .master(master)
            .getOrCreate();        
        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        LongAccumulator BLOCK_ASSIGNMENTS = jsc.sc().longAccumulator();
        
        BlockFiltering bf = new BlockFiltering(spark, blockSizesOutputPath, entityIndexOutputPath);
        JavaPairRDD<Integer, Integer[]> entityIndex = bf.run(jsc.textFile(inputPath), BLOCK_ASSIGNMENTS); //input: a blocking collection
        entityIndex.saveAsObjectFile(entityIndexOutputPath);
    }
    
}
