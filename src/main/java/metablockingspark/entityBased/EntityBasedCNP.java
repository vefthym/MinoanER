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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNP implements Serializable {
    
    public JavaPairRDD<Integer,Integer[]> run(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, Broadcast<JavaPairRDD<Integer,Double>> totalWeightsBV, int K, long numNegativeEntities, long numPositiveEntities) {
        
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
        
        JavaPairRDD<Integer,Double> totalWeightsRDD = totalWeightsBV.value();
        //System.out.println(totalWeightsRDD.count()+ " total weights found");
        
        //mapOutput is in the form : <entityId, [neighborIds]>
        JavaPairRDD<Integer,Iterable<Integer[]>> entitiesBlocks = mapOutput.groupByKey(336); //(entityId, Iterable<[neighborIds]>)     //shuffle 1 (or 5)
        
        
        
        JavaPairRDD<Integer,Integer[]> neighborsRDD; //an array of unique neighbor ids per entityId
        /*neighborsRDD = mapOutput.aggregateByKey(     //shuffle 1
                new HashSet<Integer>(), 
                (x,y) -> {x.addAll(Arrays.asList(y)); return x;}, 
                (x,y) -> {x.addAll(y); return x;}
        ).mapValues(x -> x.toArray(new Integer[x.size()]));        
        */
        // alternative for neighborsRDD
        neighborsRDD = entitiesBlocks.mapToPair(x -> {  //equivalent to mapOutput.aggregateByKey, but uses the cached entitiesBlocks
            Set<Integer> neighbors = new HashSet<>();
            for (Integer[] candidates : x._2()) {
                neighbors.addAll(Arrays.asList(candidates));
            }
            return new Tuple2<>(x._1(), neighbors.toArray(new Integer[neighbors.size()]));
        });
        
        
        //the following is very expensive, try to reduce the complexity (try to minimize shuffles)        
        return neighborsRDD.flatMapToPair(x -> {
            List<Tuple2<Integer,Integer>> reversed = new ArrayList<>();
            Integer eId = x._1();
            for (Integer neighbor : x._2()) {
                reversed.add(new Tuple2<>(neighbor, eId));
            }
            reversed.add(new Tuple2<>(eId,eId));
            return reversed.iterator();
        }) // (neighborId, eId)
        .join(totalWeightsRDD,336) // (neighborId, <eId, weight(neighborId)>        //shuffle 2
        .mapToPair(x -> {
            return new Tuple2<>(x._2()._1(), new Tuple2<>(x._1(), x._2()._2()));
        }) //<eId, <neighborId, weight(neighborId)>>        
        .groupByKey(336) //<eId, Iterable<neighborId, weight(neighborId)>>     //shuffle 3
        .join(entitiesBlocks, 336) //<eId, (<Iterable<neighborId, weight(neighborId)>, Iterable<[neighborIds]>)>     //shuffle 4   
                
        .mapToPair(x -> {
            Integer entityId = x._1();
            //compute the numerators
            Map<Integer,Double> counters = new HashMap<>(); //number of common blocks with current entity per candidate match
            for(Integer[] candidates : x._2()._2()) { //for each block (as an array of entities)              
                int numNegativeEntitiesInBlock = getNumNegativeEntitiesInBlock(candidates);
                int numPositiveEntitiesInBlock = candidates.length - numNegativeEntitiesInBlock;
                double weight1 = Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                double weight2 = Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);

                for (int neighborId : candidates) {
                    Double currWeight = counters.get(neighborId);
                    if (currWeight == null) {
                        currWeight = 0.0;
                    }				
                    counters.put(neighborId, currWeight+weight1+weight2);
                }
            }
            
            //retrive the totalWeights only for the local entities
            Map<Integer, Double> localEntityWeights = new HashMap<>();
            for (Tuple2<Integer, Double> weight : x._2()._1()) {
                localEntityWeights.put(weight._1(), weight._2());
            }            
         
            //calculate the weights of each candidate match (edge in the blocking graph)
            Map<Integer, Double> weights = new HashMap<>();
            double entityWeight = localEntityWeights.get(entityId);
            for (int neighborId : counters.keySet()) {
                double currentWeight = counters.get(neighborId) / (Double.MIN_NORMAL + entityWeight + localEntityWeights.get(neighborId));
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
        
        
        
        //the following is cheap to compute (one shuffle needed), but can easily give OOM error
        /*
        Map<Integer,Double> totalWeights = totalWeightsRDD.collectAsMap(); //cause of OOM error
        
        //reduce phase
        //metaBlockingResults: key: an entityId, value: an array of topK candidate matches, in descending order of score (match likelihood)
        return mapOutput.groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapToPair(x -> {
                    Integer entityId = x._1();
                    
                    //compute the numerators
                    Map<Integer,Double> counters = new HashMap<>(); //number of common blocks with current entity per candidate match
                    for(Integer[] candidates : x._2()) {                       
                        int numNegativeEntitiesInBlock = getNumNegativeEntitiesInBlock(candidates);
                        int numPositiveEntitiesInBlock = candidates.length - numNegativeEntitiesInBlock;
                        double weight1 = Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                        double weight2 = Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);
                        
                        for (int neighborId : candidates) {
                            Double currWeight = counters.get(neighborId);
                            if (currWeight == null) {
                                currWeight = 0.0;
                            }				
                            counters.put(neighborId, currWeight+weight1+weight2);
                        }
                    }
                    
                    //calculate the weights of each candidate match (edge in the blocking graph)
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
                */
    }
                
    private int getNumNegativeEntitiesInBlock(Integer[] candidates) {
        int count = 0;
        for (Integer candidate : candidates) {
            if (candidate < 0) {
                count++;
            }
        }
        return count;
    }
    
}
