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

package metablockingspark.entityBased.neighbors;

import metablockingspark.entityBased.*;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPNeighborsInMemory implements Serializable {
    
    /**
     * 
     * @param blocksFromEI
     * @param totalWeightsBV
     * @param K
     * @param numNegativeEntities
     * @param numPositiveEntities
     * @return key: a negative entityId, value: a list of pairs of candidate matches (positive entity ids) along with their value_sim with the key
     */
    public JavaPairRDD<Integer,Int2FloatOpenHashMap> getValueSimForNegativeEntities(JavaPairRDD<Integer, IntArrayList> blocksFromEI, Broadcast<Int2FloatOpenHashMap> totalWeightsBV, int K, long numNegativeEntities, long numPositiveEntities) {
        
        Int2FloatOpenHashMap totalWeights = totalWeightsBV.value(); //saves memory by storing data as primitive types                
        totalWeightsBV.unpersist();
        totalWeightsBV.destroy();
        
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
               
        //reduce phase
        //metaBlockingResults: key: a negative entityId, value: a list of candidate matches (positive entity ids) along with their value_sim with the key
        return mapOutput
                .filter(x -> x._1() < 0) //symmetry of value_sim allows that (value_sim(-1,8) = value_sim(8,-1)), negative ids are less than positives
                .groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapToPair(x -> {
                    Integer entityId = x._1();
                    
                    //compute the numerators
                    Int2FloatOpenHashMap counters = new Int2FloatOpenHashMap(); //number of common blocks with current entity per candidate match
                    for(IntArrayList candidates : x._2()) {                       
                        int numNegativeEntitiesInBlock = (int) candidates.stream().filter(eid -> eid<0).count();        
                        int numPositiveEntitiesInBlock = candidates.size() - numNegativeEntitiesInBlock;
                        float weight1 = (float) Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                        float weight2 = (float) Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);
                        
                        for (int neighborId : candidates) {
                            counters.addTo(neighborId, weight1+weight2);                    
                        }
                    }
                                        
                    //calculate the weight of each edge in the blocking graph (i.e., for each candidate match)
                    Int2FloatOpenHashMap weights = new Int2FloatOpenHashMap();
                    float entityWeight = totalWeights.get(entityId.intValue());
                    for (int neighborId : counters.keySet()) {
			float currentWeight = counters.get(neighborId) / (Float.MIN_NORMAL + entityWeight + totalWeights.get(neighborId));
			weights.put(neighborId, currentWeight);			
                    }
                    
                    return new Tuple2<>(entityId, weights);
                });               
    }    
    
    
    public void getNeighborSims(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, JavaPairRDD<Integer,Int2FloatOpenHashMap> valueSims, JavaPairRDD<Integer,IntArrayList> inNeighbors) {
            
        JavaPairRDD<Integer,Int2FloatOpenHashMap> neighborSims;
    }
}
