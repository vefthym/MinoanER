package metablockingspark.preprocessing;

import com.google.common.collect.Ordering;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
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
    private JavaPairRDD<String,String[]> parseBlockCollection(JavaRDD<String> blockingInput) {
        return blockingInput
            .map(line -> line.split("\t")) //split to [blockId, [entityIds]]
            .filter(line -> line.length == 2) //only keep lines of this format
            .mapToPair(line -> new Tuple2<String,String[]>(line[0], line[1].replaceFirst(";", "").split("#"))); 
    }
    
    public JavaPairRDD<Integer,Integer> getBlockSizes(JavaPairRDD<String,String[]> parsedBlocks) {
        LongAccumulator numComparisons = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
        return parsedBlocks         
            .mapToPair(pair -> {
                int blockId = Integer.parseInt(pair._1());
                String[] entities = pair._2();
                if (entities == null || entities.length == 0) {
                    return null;
                }
                int numEntities = entities.length;
                int D1counter = 0;
                for (String entity : entities) {
                    if (entity.isEmpty()) continue; //in case the last entityId finishes with '#'
                    Integer entityId = Integer.parseInt(entity);			                    
                    if (entityId >= 0) {
                            D1counter++;
                    } 
                }
                int D2counter = numEntities-D1counter;
                long blockComparisons = D1counter * D2counter;
                numComparisons.add(blockComparisons);
                
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
        
    private void getEntityIndex(JavaPairRDD<String,String[]> parsedBlocks, JavaPairRDD<Integer,Integer> blockSizes) {
        //get pairs of the form (entityId, blockId)
        JavaPairRDD<Integer, Integer> entityIndexMapperResult = parsedBlocks
            .flatMapToPair(pair -> {
                int blockId = Integer.parseInt(pair._1());
                String[] entities = pair._2();
                if (entities == null || entities.length == 0) {
                    return null;
                }
                List<Tuple2<Integer,Integer>> mapResults = new ArrayList<>(); //possible (but not really probable) cause of OOM (memory errors) if huge blocks exist
                for (String entity : entities) {
                    if (entity.isEmpty()) continue; //in case the last entityId finishes with '#'
                    Integer entityId = Integer.parseInt(entity);			                    
                    mapResults.add(new Tuple2<>(entityId, blockId));
                }
                return mapResults.iterator();
            }); //end of EntityIndexMapper logic 

        //add an integer rank to each blockId, starting with 0 (blocks are sorted in descending utility)
        Map<Integer,Long> blocksRanking = blockSizes.values().zipWithIndex().collectAsMap(); 
        
        //create the entity index (similar to EntityIndexReducer)
        entityIndexMapperResult
                .mapValues(x -> blocksRanking.get(x)) //replace all block Ids with their rank in the sorted list of blocks (by descending utility)
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
                    return new Tuple2<Integer,Integer[]>(pair._1(), entityIndex); //the entity index for the current entity
                })                
                .saveAsObjectFile(entityIndexOutputPath);
                //.saveAsTextFile(entityIndexOutputPath);
                //.saveAsNewAPIHadoopFile(entityIndexOutputPath, VIntWritable.class, VIntArrayWritable.class, SequenceFileOutputFormat.class);                
    }
    
        
    public void run(JavaRDD<String> blockingInput) {        
        JavaPairRDD<String,String[]> parsedBlocks = parseBlockCollection(blockingInput);
        parsedBlocks.cache();
        
        JavaPairRDD<Integer,Integer> blockSizes = getBlockSizes(parsedBlocks);
        blockSizes.cache();        
        blockSizes
                .map(x -> x._1()+","+ x._2())         //to remove the parentheses
                .saveAsTextFile(blockSizesOutputPath);     
        getEntityIndex(parsedBlocks, blockSizes);
    }
        
    
    private static void deletePath(String stringPath) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://master:9000"), conf);
        Path path = new Path(stringPath);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
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
                deletePath(blockSizesOutputPath);
                deletePath(entityIndexOutputPath);
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
            .master(master)
            .getOrCreate();        
        
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        BlockFiltering bf = new BlockFiltering(spark, blockSizesOutputPath, entityIndexOutputPath);
        bf.run(sc.textFile(inputPath));
    }
    
}
