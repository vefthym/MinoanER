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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNP {

    SparkSession spark;     

    public EntityBasedCNP(SparkSession spark) {
        this.spark = spark;        
    }
    
    public JavaPairRDD<Integer,Integer[]> run(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, Broadcast<Map<Integer,Double>> totalWeightsBV, int K) {
        
        Map<Integer,Double> totalWeights = totalWeightsBV.value();
        
        //map phase
        JavaPairRDD<Integer, Integer[]> mapOutput = blocksFromEI.flatMapToPair(x -> {            
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
            
            Integer[] positivesArray = positives.toArray(new Integer[positives.size()]);
            Integer[] negativesArray = negatives.toArray(new Integer[negatives.size()]);
            
            List<Tuple2<Integer,Integer[]>> mapResults = new ArrayList<>();
            
            //emit all the negative entities array for each positive entity
            for (int i = 0; i < positivesArray.length; ++i) {                
                mapResults.add(new Tuple2<>(positivesArray[i], negativesArray));                
            }

            //emit all the positive entities array for each negative entity
            for (int i = 0; i < negativesArray.length; ++i) {
                mapResults.add(new Tuple2<>(negativesArray[i], positivesArray));                
            }
            
            return mapResults.iterator();
        })
        .filter(x-> x != null);
        
        //reduce phase
        //metaBlockingResults: key: an entityId, value: an array of topK candidate matches, in descending order of score (match likelihood)
        return mapOutput.groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapToPair(x -> {
                    Integer entityId = x._1();
                    
                    //find number of common blocks
                    Map<Integer,Double> counters = new HashMap<>(); //number of common blocks with current entity per candidate match
                    for(Integer[] next : x._2()) {
                        for (int neighborId : next) {
                            Double count = counters.get(neighborId);
                            if (count == null) {
                                count = 0.0;
                            }				
                            counters.put(neighborId, count+1);
                        }
                    }
                    
                    //calculate the weights
                    Map<Integer, Double> weights = new HashMap<>();
                    double entityWeight = totalWeights.get(entityId);
                    for (int neighborId : counters.keySet()) {
			double currentWeight = counters.get(neighborId) / (Double.MIN_NORMAL + entityWeight + totalWeights.get(neighborId));
			weights.put(neighborId, currentWeight);			
                    }
                    
                    //keep the top-K weights
                    weights = Utils.sortByValue(weights);                    
                    Integer[] candidateMatchesSorted = new Integer[Math.min(weights.size(), K)];                    
                    
                    int i = 0;
                    for (Integer neighbor : weights.keySet()) {
                        if (i == weights.size() || i == K) {
                            break;
                        }
                        candidateMatchesSorted[i++] = neighbor;                        
                    }
                    
                    return new Tuple2<Integer,Integer[]>(entityId, candidateMatchesSorted);
                });
                
    }
    
}
