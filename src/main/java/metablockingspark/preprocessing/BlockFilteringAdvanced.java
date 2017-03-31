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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class BlockFilteringAdvanced {
    
    static final Logger logger = Logger.getLogger(BlockFilteringAdvanced.class.getName());
    
    public JavaPairRDD<Integer, IntArrayList> run(JavaRDD<String> blockingInput, LongAccumulator BLOCK_ASSIGNMENTS) {        
        JavaPairRDD<Integer,IntArrayList> parsedBlocks = parseBlockCollection(blockingInput);        
        
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> entityBlocks = getEntityBlocksAdvanced(parsedBlocks);       

        JavaPairRDD<Integer, IntArrayList> entityIndex = getEntityIndex(entityBlocks, BLOCK_ASSIGNMENTS);
        parsedBlocks.unpersist();
        return  entityIndex;
    }
    
    //resulting key:blockID, value:entityIds array                            
    public JavaPairRDD<Integer,IntArrayList> parseBlockCollection(JavaRDD<String> blockingInput) {
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
                return new Tuple2<>(blockId, new IntArrayList(outputEntities.stream().mapToInt(i->i).toArray()));
            })
            .filter(x -> x != null);
    }
    
    //input: a JavaPairRDD of key:blockID, value:entityIds array        
    //output: a JavaPairRDD of key:entityID, value: (blockId, blockUtility)
    private JavaPairRDD<Integer, Tuple2<Integer, Integer>> getEntityBlocksAdvanced(JavaPairRDD<Integer, IntArrayList> parsedBlocks) {
        return parsedBlocks.flatMapToPair(block -> {
            int[] entities = block._2().elements();                
            int numEntities = entities.length;
            int D1counter = 0;
            for (int entity : entities) {                                        
                if (entity >= 0) {
                    D1counter++;
                } 
            }
            int D2counter = numEntities-D1counter;
            long blockComparisons = D1counter * D2counter;
            
            int inverseUtility = Math.max(D1counter, D2counter);
            if (blockComparisons == 0) {
                inverseUtility = 0;
            } 
            Tuple2<Integer,Integer> blockUtility = new Tuple2<>(block._1(), inverseUtility);  
            
            List<Tuple2<Integer, Tuple2<Integer,Integer>>> mapResults = new ArrayList<>();
            for (Integer entityId : entities) {
                mapResults.add(new Tuple2<>(entityId, blockUtility));
            }
            return mapResults.iterator();
        });
    }
    
    
    //input: a JavaPairRDD of key:entityID, value: (blockId, blockUtility)
    //output: a JavaPairRDD of key:entityId, value: [blockIds] (filtered), i.e., an entity index
    private JavaPairRDD<Integer, IntArrayList> getEntityIndex(JavaPairRDD<Integer,Tuple2<Integer,Integer>> entityBlocks,  LongAccumulator BLOCK_ASSIGNMENTS) {        
        System.out.println("Creating the entity index...");
        
        return entityBlocks.groupByKey()
            .mapValues(blocks -> {               
                final int MAX_BLOCKS = ((Double)Math.floor(3*blocks.spliterator().getExactSizeIfKnown()/4+1)).intValue(); //|_ 3|Bi|/4+1 _| //preprocessing
                //sort the tuples by value (inverseUtility)
                Map<Integer,Integer> inverseBlocks = new TreeMap<>(); 
                for (Tuple2<Integer,Integer> block : blocks) {
                    inverseBlocks.put(block._2(), block._1());                        
                }

                //keep MAX_BLOCKS blocks per entity
                List<Integer> blocksKept = new ArrayList<>();
                int indexedBlocks = 0;
                for (Integer blockId : inverseBlocks.values()) {
                    blocksKept.add(blockId);
                    if (++indexedBlocks == MAX_BLOCKS) { break;} //comment-out this line to skip block filtering
                }
                IntArrayList entityIndex = new IntArrayList(blocksKept.stream().mapToInt(i->i).toArray());                
                BLOCK_ASSIGNMENTS.add(entityIndex.size());

                return entityIndex;
            });    
    }
    
}
