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

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityWeightsWJS implements Serializable {
    
    private long numPositiveEntities, numNegativeEntities;

    public EntityWeightsWJS() {
        numNegativeEntities = 0;
        numPositiveEntities = 0;        
    }
            
    public long getNumPositiveEntities() {
        return numPositiveEntities;
    }
    
    public long getNumNegativeEntities() {
        return numNegativeEntities;
    }
    
    public JavaPairRDD<Integer, Double> getWeights(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, JavaPairRDD<Integer,Integer[]> entityIndex) {
        JavaRDD<Integer> entityIds = entityIndex.keys().cache();
        
        System.out.println("Getting blocksFromEI as a Map...");
        blocksFromEI.persist(StorageLevel.DISK_ONLY());
        Map<Integer, Integer> blockSizesMap = blocksFromEI
                .mapValues(x -> (int) x.spliterator().getExactSizeIfKnown())                
                .collectAsMap();
        
        Map<Integer, Integer> numNegativeEntitiesInBlock = 
                blocksFromEI
                .mapValues(x -> getNumberOfNegativeEntitiesInBlock(x))
                .collectAsMap();
        
        //Map<Integer, Iterable<Integer>> blocksMap = blocksFromEI.collectAsMap();  //key: blockId, value: entityIds
        /*if (blocksMap == null || blocksMap.isEmpty()) {
            System.err.println("ERROR: NO blocks were written in blocksFromEI!");
            System.exit(0);
        }*/
                
        //JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI = blocksFromEI_BV.value();
        
        System.out.println("Counting positive and negative entities...");
        numNegativeEntities = entityIds.filter(x -> x < 0).count();
        numPositiveEntities = entityIds.count() - numNegativeEntities;
                       
        System.out.println("Computing weights and storing them in the resulting RDD...");
        return entityIndex.mapToPair(entityInfo -> {
            int entityId = entityInfo._1();
            Integer[] associatedBlocks = entityInfo._2();
            double totalWeight = 0;            
            if (entityId < 0) {
                for (int block : associatedBlocks) {                    
                    /*Iterable<Integer> blockContents = blocksMap.get(block);
                    if (blockContents == null) {
                        System.out.println("blocksMap does not contain block "+block);
                        continue;
                    }
                    int df2t = getNumberOfNegativeEntitiesInBlock(blockContents);*/
                    Integer df2t = numNegativeEntitiesInBlock.get(block);
                    if (df2t == null) {
                        continue;
                    }                    
                    double weight2 = Math.log10((double) numNegativeEntities / df2t); //IDF                    
                    totalWeight += weight2;                                
                }
            } else {
                for (int block : associatedBlocks) {                    
                    /*
                    Iterable<Integer> blockContents = blocksMap.get(block);
                    if (blockContents == null) {
                        continue;
                    }
                    int df1t = (int) blockContents.spliterator().getExactSizeIfKnown() - getNumberOfNegativeEntitiesInBlock(blockContents);
                    */
                    Integer blockSize = blockSizesMap.get(block);
                    if (blockSize == null) {
                        continue;
                    }
                    Integer negativesInBlock = numNegativeEntitiesInBlock.get(block);
                    if (negativesInBlock == null) {
                        continue;
                    }
                    int df1t = blockSize - negativesInBlock;                    
                    double weight1 = Math.log10((double) numPositiveEntities / df1t); //IDF                    
                    totalWeight += weight1;
                    
                }                
            }
            return new Tuple2<>(entityId, totalWeight);
        });//.collectAsMap();
        
        //System.out.println("totalWeights contains "+ totalWeights.size()+" entities");
        //return totalWeights;
    }
    
    private int getNumberOfNegativeEntitiesInBlock(Iterable<Integer> blockContents) {
        int numNegatives = 0;
        for (Integer entityId : blockContents) {
            if (entityId < 0) {
                numNegatives++;
            }
        }
        return numNegatives;
    }
    
}
