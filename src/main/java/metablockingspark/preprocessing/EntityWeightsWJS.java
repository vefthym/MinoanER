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
    
    public JavaPairRDD<Integer, Float> getWeights(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, JavaPairRDD<Integer,Integer[]> entityIndex) {
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
        
        
        System.out.println("Counting positive and negative entities...");
        numNegativeEntities = entityIds.filter(x -> x < 0).count();
        numPositiveEntities = entityIds.count() - numNegativeEntities;
        entityIds.unpersist();
                       
        System.out.println("Computing weights and storing them in the resulting RDD...");
        return entityIndex.mapToPair(entityInfo -> {
            int entityId = entityInfo._1();
            Integer[] associatedBlocks = entityInfo._2();
            float totalWeight = 0;            
            if (entityId < 0) {
                for (int block : associatedBlocks) {                    
                    Integer df2t = numNegativeEntitiesInBlock.get(block);
                    if (df2t == null) {
                        continue;
                    }                    
                    float weight2 = (float) Math.log10((double) numNegativeEntities / df2t); //IDF                    
                    totalWeight += weight2;                                
                }
            } else {
                for (int block : associatedBlocks) {                    
                    Integer blockSize = blockSizesMap.get(block);
                    if (blockSize == null) {
                        continue;
                    }
                    Integer negativesInBlock = numNegativeEntitiesInBlock.get(block);
                    if (negativesInBlock == null) {
                        continue;
                    }
                    int df1t = blockSize - negativesInBlock;                    
                    float weight1 = (float) Math.log10((double) numPositiveEntities / df1t); //IDF                    
                    totalWeight += weight1;
                    
                }                
            }
            return new Tuple2<>(entityId, totalWeight);
        });
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
