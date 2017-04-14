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

import it.unimi.dsi.fastutil.ints.Int2FloatLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import metablockingspark.entityBased.*;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import metablockingspark.relationsWeighting.RelationsRank;
import metablockingspark.utils.ComparableIntFloatPair;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPNeighborsUnnormalized implements Serializable {
    
    
    /**
     * 
     * @param topKvalueCandidates the topK results per entity, acquired from value similarity
     * @param rawTriples1 the rdf triples of the first entity collection
     * @param rawTriples2 the rdf triples of the second entity collection
     * @param SEPARATOR the delimiter that separates subjects, predicates and objects in the rawTriples1 and rawTriples2 files
     * @param entityIds1 the mapping of entity urls to entity ids, as it was used in blocking
     * @param entityIds2
     * @param MIN_SUPPORT_THRESHOLD the minimum support threshold, below which, relations are discarded from top relations
     * @param K the K for topK candidate matches
     * @param N the N for topN rdf relations (and neighbors)
     * @param jsc the java spark context used to load files and broadcast variables
     * @return topK neighbor candidates per entity
     */
    public JavaPairRDD<Integer, IntArrayList> run(JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> topKvalueCandidates, 
            JavaRDD<String> rawTriples1, 
            JavaRDD<String> rawTriples2,             
            String SEPARATOR, 
            JavaRDD<String> entityIds1, 
            JavaRDD<String> entityIds2, 
            float MIN_SUPPORT_THRESHOLD,
            int K,
            int N, 
            JavaSparkContext jsc) {
        
        Map<Integer,IntArrayList> inNeighbors = new HashMap<>(new RelationsRank().run(rawTriples1, SEPARATOR, entityIds1, MIN_SUPPORT_THRESHOLD, N, true, jsc));
        inNeighbors.putAll(new RelationsRank().run(rawTriples2, SEPARATOR, entityIds2, MIN_SUPPORT_THRESHOLD, N, false, jsc));
        
        Broadcast<Map<Integer,IntArrayList>> inNeighbors_BV = jsc.broadcast(inNeighbors);

        //JavaPairRDD<Integer, IntArrayList> topKneighborCandidates =  getTopKNeighborSims(topKvalueCandidates, inNeighbors_BV, K);        
        JavaPairRDD<Integer, IntArrayList> topKneighborCandidates =  getTopKNeighborSimsSUM(topKvalueCandidates, inNeighbors_BV, K);        
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
    public JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> getTopKValueSims(JavaPairRDD<Integer, IntArrayList> blocksFromEI, int K, long numNegativeEntities, long numPositiveEntities) {                
    
        //key: an entityId, value: a list of candidate matches, with first number being the number of entities from the same collection in this block
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutputWJS(blocksFromEI);
               
        //reduce phase
        //metaBlockingResults: key: a negative entityId, value: a list of candidate matches (positive entity ids) along with their value_sim with the key
        return mapOutput
                .groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)                
                .mapToPair(x -> {                    
                    int entityId = x._1();
                    
                    //compute the numerators
                    Int2FloatOpenHashMap counters = new Int2FloatOpenHashMap(); //number of common blocks with current entity per candidate match
                    for(IntArrayList candidates : x._2()) {                        
                        int numNegativeEntitiesInBlock = candidates.getInt(0); //the first element is the number of entities from the same collection
                        int numPositiveEntitiesInBlock = candidates.size()-1; //all the other candidates are positive entity ids
                        if (entityId >= 0) {
                            numPositiveEntitiesInBlock = candidates.getInt(0);
                            numNegativeEntitiesInBlock = candidates.size()-1;
                        }
                        float weight1 = (float) Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                        float weight2 = (float) Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);
                        
                        candidates = new IntArrayList(candidates.subList(1, candidates.size())); //remove the first element which is the number of entities in this block from the same collection as the entityId
                        
                        for (int candidateId : candidates) {
                            counters.addTo(candidateId, weight1+weight2);                    
                        }
                    }
                    
                    //keep the top-K weights
                    Int2FloatLinkedOpenHashMap weights = new Int2FloatLinkedOpenHashMap(Utils.sortByValue(counters, true));
                    Int2FloatLinkedOpenHashMap weightsToEmit = new Int2FloatLinkedOpenHashMap();                                      
                    int i = 0;                    
                    for (Map.Entry<Integer, Float> neighbor : weights.entrySet()) {                        
                        if (i == weights.size() || i == K) {
                            break;
                        }
                        weightsToEmit.put(neighbor.getKey().intValue(), weights.get(neighbor.getKey().intValue()));
                        i++;
                    }
                    
                    return new Tuple2<>(entityId, weightsToEmit);
                })
                .filter(x-> !x._2().isEmpty());
    }    
    
    
    public JavaPairRDD<Integer, IntArrayList> getTopKNeighborSims (JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> valueSims, Broadcast<Map<Integer,IntArrayList>> inNeighbors_BV, int K) {
        return valueSims.flatMapToPair(x->{
            int eId = x._1();
            IntArrayList eInNeighbors = inNeighbors_BV.value().get(eId);
            
            List<Tuple2<Integer,ComparableIntFloatPair>> partialNeighborSims = new ArrayList<>(); //key: entityId, value: (candidateId, valueSim(outNeighbor(eId),outNeighbor(cId)) )
            if (eInNeighbors == null) {
                return partialNeighborSims.iterator(); //empty
            }
            for (Map.Entry<Integer, Float> eIdValueCandidates : x._2().entrySet()) { //for each candidate match of eId from values
                IntArrayList inNeighborsOfCandidate = inNeighbors_BV.value().get(eIdValueCandidates.getKey());
                if (inNeighborsOfCandidate == null) {
                    continue; //go to next candidate match. this one does not have in-neighbors
                }
                Float tmpNeighborSim = eIdValueCandidates.getValue();
                for (Integer inNeighborOfCandidate : inNeighborsOfCandidate) { //for each in-neighbor of the candidate match of the current entity                    
                    for (Integer eInNeighbor : eInNeighbors) {  //for each in-neighbor of the current entity
                        partialNeighborSims.add(new Tuple2<>(eInNeighbor, new ComparableIntFloatPair(inNeighborOfCandidate, tmpNeighborSim)));
                        partialNeighborSims.add(new Tuple2<>(inNeighborOfCandidate, new ComparableIntFloatPair(eInNeighbor, tmpNeighborSim)));                        
                    }
                }
            }
            
            return partialNeighborSims.iterator();
        })
        //keep top-K candidates per (key) entity
        .combineByKey(//should be faster than groupByKey (keeps local top-Ks before shuffling, like a combiner in MapReduce)
            //createCombiner
            x-> {
                PriorityQueue<ComparableIntFloatPair> initial = new PriorityQueue<>();
                initial.add(x);
                return initial; 
            }
            //mergeValue
            , (PriorityQueue<ComparableIntFloatPair> pq, ComparableIntFloatPair x) -> {
                pq.add(x); 
                pq = removeSamePairWithLowerValue(pq, x);                
                if (pq.size() > K) {
                    pq.poll();
                }
                return pq;
            }
            //mergeCombiners
            , (PriorityQueue<ComparableIntFloatPair> pq1, PriorityQueue<ComparableIntFloatPair> pq2) -> {
                while (!pq2.isEmpty()) {
                    ComparableIntFloatPair c = pq2.poll();
                    pq1.add(c);
                    pq1 = removeSamePairWithLowerValue(pq1, c);                    
                    if (pq1.size() > K) {
                        pq1.poll();
                    }
                }
                return pq1;
            }
        ).mapValues(pq -> Utils.toIntArrayListReversed(pq));
    }

    /**
     * At this point, pq contains x and maybe 1 more element y with the same key as x. If y exists, keep from those two the one with the better value. 
     * If y does not exist, keep x. 
     * @param pq
     * @param x
     * @return 
     */
    private PriorityQueue<ComparableIntFloatPair> removeSamePairWithLowerValue(PriorityQueue<ComparableIntFloatPair> pq, ComparableIntFloatPair x) {
        int entityIdToAdd = x.getEntityId();
        float newValue = x.getValue();
        ComparableIntFloatPair elementToDelete = null;
        boolean sameValueTwice = false;
        for (ComparableIntFloatPair qElement : pq) { //traverses the queue in random order
            if (qElement.getEntityId() == entityIdToAdd) {
                if (qElement.getValue() < newValue) { //y is worse than x => delete y
                    elementToDelete = qElement;
                    break;
                } else if (qElement.getValue() > newValue) { //y is better than x => delete x
                    elementToDelete = x;
                    break;
                } else {  //qElement has the same value as x (or is x)
                    if (!sameValueTwice) { //first time meeting this element (it can be x or a y with the same rank)
                        sameValueTwice = true; 
                    } else{               //second time meeting this element (x and y are equivalent => delete one of them)
                        elementToDelete = x;
                        break;
                    }
                }
            }
        }
        if (elementToDelete != null) {
            pq.remove(elementToDelete);
        }
        return pq;
    }
    
    public JavaPairRDD<Integer, IntArrayList> getTopKNeighborSimsSUM (JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> valueSims, Broadcast<Map<Integer,IntArrayList>> inNeighbors_BV, int K) {
        return valueSims.flatMapToPair(x->{
            int eId = x._1();
            IntArrayList eInNeighbors = inNeighbors_BV.value().get(eId);
            
            List<Tuple2<Integer,ComparableIntFloatPair>> partialNeighborSims = new ArrayList<>(); //key: entityId, value: (candidateId, valueSim(outNeighbor(eId),outNeighbor(cId)) )
            if (eInNeighbors == null) {
                return partialNeighborSims.iterator(); //empty
            }
            for (Map.Entry<Integer, Float> eIdValueCandidates : x._2().entrySet()) { //for each candidate match of eId from values
                IntArrayList inNeighborsOfCandidate = inNeighbors_BV.value().get(eIdValueCandidates.getKey());
                if (inNeighborsOfCandidate == null) {
                    continue; //go to next candidate match. this one does not have in-neighbors
                }
                Float tmpNeighborSim = eIdValueCandidates.getValue();                
                for (Integer inNeighborOfCandidate : inNeighborsOfCandidate) { //for each in-neighbor of the candidate match of the current entity                    
                    for (Integer eInNeighbor : eInNeighbors) {  //for each in-neighbor of the current entity
                        partialNeighborSims.add(new Tuple2<>(eInNeighbor, new ComparableIntFloatPair(inNeighborOfCandidate, tmpNeighborSim)));
                        partialNeighborSims.add(new Tuple2<>(inNeighborOfCandidate, new ComparableIntFloatPair(eInNeighbor, tmpNeighborSim)));                        
                    }
                }
            }
            
            return partialNeighborSims.iterator();
        })
        .combineByKey(
            //createCombiner
            x-> {
                Int2FloatOpenHashMap initial = new Int2FloatOpenHashMap();
                initial.put(x.getEntityId(), x.getValue());
                return initial; 
            }
            //mergeValue
            , (Int2FloatOpenHashMap existingSims, ComparableIntFloatPair x) -> {
                existingSims.addTo(x.getEntityId(), x.getValue()); //sum the value sims of their out-neighbors                
                return existingSims;
            }
            //mergeCombiners
            , (Int2FloatOpenHashMap sims1, Int2FloatOpenHashMap sims2) -> {
                for (Map.Entry<Integer, Float> x : sims2.entrySet()) {
                    sims1.addTo(x.getKey(), x.getValue()); //sum the value sims of their out-neighbors                                    
                }
                return sims1;
            }
        )
        .mapValues(x -> new Int2FloatLinkedOpenHashMap(Utils.sortByValue(x, true)))
        .mapValues(x -> {      //keep the top-K candidates, based on their value
            int i = Math.min(K, x.size());   
            int[] candidates = new int[i]; 
            int j = 0;
            for (Map.Entry<Integer, Float> candidate : x.entrySet()) {
                candidates[j++] = candidate.getKey();
                if (i == j) { //K elements (or all elements, if less than K) have been added
                    break;
                }
            }
            return new IntArrayList(candidates);
        });
    }
  
}
