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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import metablockingspark.relationsWeighting.RelationsRank;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPNeighborsInMemory implements Serializable {
    
    
    public JavaPairRDD<Integer, IntArrayList> run(JavaPairRDD<Integer,Int2FloatOpenHashMap> topKvalueCandidates, 
            JavaRDD<String> rawTriples1, 
            JavaRDD<String> rawTriples2, 
            String SEPARATOR, 
            float MIN_SUPPORT_THRESHOLD,
            int K,
            int N) {
        
        Map<Integer,IntArrayList> inNeighbors = new HashMap<>(new RelationsRank().run(rawTriples1, SEPARATOR, MIN_SUPPORT_THRESHOLD, N, true));
        inNeighbors.putAll(new RelationsRank().run(rawTriples2, SEPARATOR, MIN_SUPPORT_THRESHOLD, N, false));
        
        JavaPairRDD<Tuple2<Integer, Integer>, Float> neighborSims = getNeighborSims(topKvalueCandidates, inNeighbors);
        
        JavaPairRDD<Integer, IntArrayList> topKneighborCandidates =  getTopKNeighborSims(neighborSims, K);        
        return topKneighborCandidates;
    }
    
    
    
    /**
     * 
     * @param blocksFromEI
     * @param totalWeightsBV
     * @param K
     * @param numNegativeEntities
     * @param numPositiveEntities
     * @return key: an entityId, value: a list of pairs of candidate matches along with their value_sim with the key
     */
    public JavaPairRDD<Integer,Int2FloatOpenHashMap> getTopKValueSims(JavaPairRDD<Integer, IntArrayList> blocksFromEI, Broadcast<Int2FloatOpenHashMap> totalWeightsBV, int K, long numNegativeEntities, long numPositiveEntities) {
        
        Int2FloatOpenHashMap totalWeights = totalWeightsBV.value(); //saves memory by storing data as primitive types                
        totalWeightsBV.unpersist();
        totalWeightsBV.destroy();
        
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
               
        //reduce phase
        //metaBlockingResults: key: a negative entityId, value: a list of candidate matches (positive entity ids) along with their value_sim with the key
        return mapOutput
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
                    Map<Integer, Float> weights = new HashMap<>();
                    float entityWeight = totalWeights.get(entityId.intValue());
                    for (int neighborId : counters.keySet()) {
			float currentWeight = counters.get(neighborId) / (Float.MIN_NORMAL + entityWeight + totalWeights.get(neighborId));
			weights.put(neighborId, currentWeight);			
                    }
                    
                    //keep the top-K weights
                    weights = Utils.sortByValue(weights);                                        
                    Int2FloatOpenHashMap weightsToEmit = new Int2FloatOpenHashMap();                                      
                    int i = 0;
                    for (int neighbor : weights.keySet()) {
                        if (i == weights.size() || i == K) {
                            break;
                        }
                        weightsToEmit.put(neighbor, (float)weights.get(neighbor));
                    }
                    
                    return new Tuple2<>(entityId, weightsToEmit);
                });          
    }    
    
    
    /**
     * Returns the neighborSim of each entity pair. 
     * @param valueSims an RDD with the top-K value sims for each entity, in the form key: eid, value: (candidateMatch cId, value_sim(eId,cId))
     * @param inNeighbors a Map with the top in-neighbors of each entity (the reverse of top-3 out-Neighbors). Possible cause of OutOfMemory error (try to make as compact as possible)
     * @return the neighborSim of each entity pair, where entity pair is the key and neighborSim is the value
     */
    public JavaPairRDD<Tuple2<Integer, Integer>, Float> getNeighborSims(JavaPairRDD<Integer,Int2FloatOpenHashMap> valueSims, Map<Integer,IntArrayList> inNeighbors) {
        
       return valueSims.flatMapToPair(x->{
            int eId = x._1();
            IntArrayList eInNeighbors = inNeighbors.get(eId);
            
            List<Tuple2<Tuple2<Integer,Integer>, Float>> partialNeighborSims = new ArrayList<>(); //key: (negativeEid, positiveEid), value: valueSim(outNeighbor(nEid),outNeighbor(pEid))
            for (Map.Entry<Integer, Float> eIdValueCandidates : x._2().entrySet()) {
                for (Integer eInNeighbor : eInNeighbors) {
                    if (eId < 0) 
                        partialNeighborSims.add(new Tuple2<>(new Tuple2<>(eInNeighbor, eIdValueCandidates.getKey()), eIdValueCandidates.getValue()));
                    else 
                        partialNeighborSims.add(new Tuple2<>(new Tuple2<>(eIdValueCandidates.getKey(), eInNeighbor), eIdValueCandidates.getValue()));
                }
            }
            
            return partialNeighborSims.iterator();
        })
        .reduceByKey((w1, w2) -> Math.max(w1, w2)); //for each entity pair, neighborSim = max value sim of its pairs of out-neighbors        
    }
    
    
    public JavaPairRDD<Integer, IntArrayList> getTopKNeighborSims(JavaPairRDD<Tuple2<Integer, Integer>, Float> neighborSims, int K) {
        return neighborSims.flatMapToPair(pair -> { //for each pair (e1,e2), value_sim(e1,e2)
            List<Tuple2<Integer, Tuple2<Integer,Float>>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(pair._1()._1(), new Tuple2<>(pair._1()._2(), pair._2()))); //emit e1, (e2,value_sim(e1,e2))
            pairs.add(new Tuple2<>(pair._1()._2(), new Tuple2<>(pair._1()._1(), pair._2()))); //emit e2, (e1,value_sim(e1,e2))
            return pairs.iterator();
        }).combineByKey( //should be faster than groupByKey (keeps local top-Ks before shuffling, like a combiner in MapReduce)
            //createCombiner
            x-> {
                PriorityQueue<CustomCandidate> initial = new PriorityQueue<>(K);
                initial.add(new CustomCandidate(x));
                return initial; 
            }
            //mergeValue
            , (PriorityQueue<CustomCandidate> pq, Tuple2<Integer,Float> tuple) -> {
                pq.add(new CustomCandidate(tuple));
                if (pq.size() > K) {
                    pq.poll();
                }
                return pq;
            }
            //mergeCombiners
            , (PriorityQueue<CustomCandidate> pq1, PriorityQueue<CustomCandidate> pq2) -> {
                while (!pq2.isEmpty()) {
                    CustomCandidate c = pq2.poll();
                    pq1.add(c);
                    if (pq1.size() > K) {
                        pq1.poll();
                    }
                }
                return pq1;
            }
        ).mapValues(x -> {      //just reverse the order of candidates and transform values to IntArrayList (topK are kept already)      
            int i = x.size();            
            IntArrayList candidates = new IntArrayList(x.size());            
            while (!x.isEmpty()) {
                candidates.add(--i, x.poll().getEntityId()); //get pq elements in reverse (i.e., descending neighbor sim) order
            }
            return new IntArrayList(candidates);
        });
    }

/**
 * Copied (and altered) from http://stackoverflow.com/a/16297127/2516301
 */
private static class CustomCandidate implements Comparable<CustomCandidate>, java.io.Serializable {
    // public final fields ok for this small example
    public final int entityId;
    public final float value;

    public CustomCandidate(Tuple2<Integer,Float> input) {
        this.entityId = input._1();
        this.value = input._2();
    }

    @Override
    public int compareTo(CustomCandidate other) {
        // define sorting according to float fields
        return Float.compare(value, other.value); 
    }
    
    public int getEntityId(){
        return entityId;
    }
    
    @Override
    public String toString() {
        return entityId+":"+value;
    }
}        
    
    
}
