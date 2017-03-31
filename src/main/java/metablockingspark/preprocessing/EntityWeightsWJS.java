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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.Serializable;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityWeightsWJS implements Serializable {
    
    private long numPositiveEntities, numNegativeEntities;
    private Int2IntOpenHashMap blockSizesMap;

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
    
    public Int2IntOpenHashMap getBlockSizesMap() {
        return blockSizesMap;
    }
    
    /**
     * Returns the total weight of each entity (i.e., the sum of its tokens' weights).
     * @param blocksFromEI the blocks after block filtering, in the form: blockId, [entityIds]
     * @param entityIndex the entity index after block filtering, in the form: entityId, [blockIds]
     * @return the total weight of each entity (i.e., the sum of its tokens' weights)
     */
    public JavaPairRDD<Integer, Float> getWeights(JavaPairRDD<Integer, IntArrayList> blocksFromEI, JavaPairRDD<Integer,IntArrayList> entityIndex) {
        JavaRDD<Integer> entityIds = entityIndex.keys().cache();
        
        System.out.println("Getting blocksFromEI as a Map...");        
        blockSizesMap = new Int2IntOpenHashMap(blocksFromEI
                .mapValues(x -> x.size())                
                .collectAsMap());
        
        Int2IntOpenHashMap numNegativeEntitiesInBlock = new Int2IntOpenHashMap(blocksFromEI
                .mapValues(x -> (int)x.stream().filter(eId->eId<0).count())
                .collectAsMap());
        
        
        System.out.println("Counting positive and negative entities...");
        numNegativeEntities = entityIds.filter(x -> x < 0).count();
        numPositiveEntities = entityIds.count() - numNegativeEntities;
        entityIds.unpersist();
                       
        System.out.println("Computing weights and storing them in the resulting RDD...");
        return entityIndex.mapToPair(entityInfo -> {
            int entityId = entityInfo._1();
            int[] associatedBlocks = entityInfo._2().elements();
            float totalWeight = 0;            
            if (entityId < 0) {
                for (int block : associatedBlocks) {                    
                    int df2t = numNegativeEntitiesInBlock.getOrDefault(block, -1);                    
                    if (df2t == -1) {
                        continue;
                    }                    
                    float weight2 = (float) Math.log10((double) numNegativeEntities / df2t); //IDF                    
                    totalWeight += weight2;                                
                }
            } else {
                for (int block : associatedBlocks) {                    
                    int blockSize = blockSizesMap.getOrDefault(block, -1);
                    if (blockSize == -1) {
                        continue;
                    }
                    int negativesInBlock = numNegativeEntitiesInBlock.getOrDefault(block, -1);
                    if (negativesInBlock == -1) {
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
}
