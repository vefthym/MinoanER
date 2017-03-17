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

package metablockingspark.entityBased;

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPInMemory implements Serializable {
    
    public JavaPairRDD<Integer,IntArrayList> run(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, Broadcast<JavaPairRDD<Integer,Float>> totalWeightsBV, int K, long numNegativeEntities, long numPositiveEntities) {
        
        //map phase
        JavaPairRDD<Integer, IntArrayList> mapOutput = blocksFromEI.flatMapToPair(x -> {            
            List<Integer> positives = new ArrayList<>();
            List<Integer> negatives = new ArrayList<>();
		
            for (int entityId : x._2()) { 
                if (entityId < 0) {
                    negatives.add(entityId);
                } else {
                    positives.add(entityId);
                }
            }
            if (positives.isEmpty() || negatives.isEmpty()) {
                return null;
            }
            
            int[] positivesArray = positives.stream().mapToInt(i->i).toArray();
            int[] negativesArray = negatives.stream().mapToInt(i->i).toArray();
            
            IntArrayList positivesToEmit = new IntArrayList(positivesArray);
            IntArrayList negativesToEmit = new IntArrayList(negatives.stream().mapToInt(i->i).toArray());
            
            List<Tuple2<Integer,IntArrayList>> mapResults = new ArrayList<>();
            
            //emit all the negative entities array for each positive entity
            for (int i = 0; i < positivesArray.length; ++i) {                
                mapResults.add(new Tuple2<>(positivesArray[i], negativesToEmit));                
            }

            //emit all the positive entities array for each negative entity
            for (int i = 0; i < negativesArray.length; ++i) {
                mapResults.add(new Tuple2<>(negativesArray[i], positivesToEmit));                
            }
            
            return mapResults.iterator();
        })
        .filter(x-> x != null);
        
        JavaPairRDD<Integer,Float> totalWeightsRDD = totalWeightsBV.value();
        //System.out.println(totalWeightsRDD.count()+ " total weights found");
        
        Int2FloatOpenHashMap totalWeights = new Int2FloatOpenHashMap(); //saves memory by storing data as primitive types        
        totalWeightsRDD.foreach(entry -> {
            totalWeights.put(entry._1().intValue(), entry._2().floatValue());
        });
        totalWeightsBV.unpersist();
        
        //the following is cheap to compute (one shuffle needed), but can easily give OOM error        
        //Map<Integer,Float> totalWeights = totalWeightsRDD.collectAsMap(); //possible cause of OOM error
        
        //reduce phase
        //metaBlockingResults: key: an entityId, value: an array of topK candidate matches, in descending order of score (match likelihood)
        return mapOutput.groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapToPair(x -> {
                    Integer entityId = x._1();
                    
                    //compute the numerators
                    Int2FloatOpenHashMap counters = new Int2FloatOpenHashMap(); //number of common blocks with current entity per candidate match
                    for(IntArrayList candidates : x._2()) {                       
                        int numNegativeEntitiesInBlock = getNumNegativeEntitiesInBlock(candidates);
                        int numPositiveEntitiesInBlock = candidates.size() - numNegativeEntitiesInBlock;
                        float weight1 = (float) Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                        float weight2 = (float) Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);
                        
                        for (int neighborId : candidates) {
                            counters.addTo(neighborId, weight1+weight2);                    
                        }
                    }
                    
                    //calculate the weights of each candidate match (edge in the blocking graph)
                    Map<Integer, Float> weights = new HashMap<>();
                    float entityWeight = totalWeights.get(entityId.intValue());
                    for (int neighborId : counters.keySet()) {
			float currentWeight = counters.get(neighborId) / (Float.MIN_NORMAL + entityWeight + totalWeights.get(neighborId));
			weights.put(neighborId, currentWeight);			
                    }
                    
                    //keep the top-K weights
                    weights = Utils.sortByValue(weights);                    
                    int[] candidateMatchesSorted = new int[Math.min(weights.size(), K)];                    
                    
                    int i = 0;
                    for (int neighbor : weights.keySet()) {
                        if (i == weights.size() || i == K) {
                            break;
                        }
                        candidateMatchesSorted[i++] = neighbor;                        
                    }
                    
                    return new Tuple2<Integer,IntArrayList>(entityId, new IntArrayList(candidateMatchesSorted));
                });               
    }
    
    private int getNumNegativeEntitiesInBlock(IntArrayList candidates) {        
        return (int) candidates.stream().filter(x -> x<0).count();        
    }
    
}
