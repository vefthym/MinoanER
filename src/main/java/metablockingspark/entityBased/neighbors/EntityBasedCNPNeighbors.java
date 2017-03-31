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
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPNeighbors implements Serializable {
    
    
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
    public JavaPairRDD<Integer, IntArrayList> run(JavaPairRDD<Integer,Int2FloatOpenHashMap> topKvalueCandidates, 
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
        
        //JavaPairRDD<Tuple2<Integer, Integer>, Float> neighborSims = getNeighborSims(topKvalueCandidates, inNeighbors_BV);        
        //JavaPairRDD<Integer, IntArrayList> topKneighborCandidates =  getTopKNeighborSims(neighborSims, K);        
        JavaPairRDD<Integer, IntArrayList> topKneighborCandidates =  getTopKNeighborSimsNEW(topKvalueCandidates, inNeighbors_BV, K);        
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
                        if (candidates.isEmpty()) continue; //still don't know why we need this line... such cases should have been filtered
                        int numNegativeEntitiesInBlock = candidates.getInt(0); //the first element is the number of entities from the same collection
                        int numPositiveEntitiesInBlock = candidates.size()-1; //all the other candidates are positive entity ids
                        if (entityId >= 0) {
                            numPositiveEntitiesInBlock = candidates.getInt(0);
                            numNegativeEntitiesInBlock = candidates.size()-1;
                        }
                        float weight1 = (float) Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                        float weight2 = (float) Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);
                        
                        candidates.removeInt(0); //remove the first element which is the number of entities in this block from the same collection as the entityId
                        
                        for (int candidateId : candidates) {
                            counters.addTo(candidateId, weight1+weight2);                    
                        }
                    }
                                        
                    //calculate the weight of each edge in the blocking graph (i.e., for each candidate match)
                    Map<Integer, Float> weights = new HashMap<>();
                    float entityWeight = totalWeightsBV.value().get(entityId);
                    for (int candidateId : counters.keySet()) {
			float currentWeight = counters.get(candidateId) / (Float.MIN_NORMAL + entityWeight + totalWeightsBV.value().get(candidateId));
			weights.put(candidateId, currentWeight);			
                    }
                    
                    //keep the top-K weights
                    weights = Utils.sortByValue(weights, true);                                        
                    Int2FloatOpenHashMap weightsToEmit = new Int2FloatOpenHashMap();                                      
                    int i = 0;
                    for (int neighbor : weights.keySet()) {                        
                        if (i == weights.size() || i == K) {
                            break;
                        }
                        weightsToEmit.put(neighbor, (float)weights.get(neighbor));
                        i++;
                    }
                    
                    return new Tuple2<>(entityId, weightsToEmit);
                });
    }    
    
    
    /**
     * Returns the neighborSim of each entity pair. 
     * @param valueSims an RDD with the top-K value sims for each entity, in the form key: eid, value: (candidateMatch cId, value_sim(eId,cId))
     * @param inNeighbors_BV
     * @return the neighborSim of each entity pair, where entity pair is the key and neighborSim is the value
     */
    public JavaPairRDD<Tuple2<Integer, Integer>, Float> getNeighborSims(JavaPairRDD<Integer,Int2FloatOpenHashMap> valueSims, Broadcast<Map<Integer,IntArrayList>> inNeighbors_BV) {
       
       return valueSims.flatMapToPair(x->{
            int eId = x._1();
            IntArrayList eInNeighbors = inNeighbors_BV.value().get(eId);
            
            List<Tuple2<Tuple2<Integer,Integer>, Float>> partialNeighborSims = new ArrayList<>(); //key: (negativeEid, positiveEid), value: valueSim(outNeighbor(nEid),outNeighbor(pEid))
            if (eInNeighbors == null) {
                return partialNeighborSims.iterator(); //empty
            }
            for (Map.Entry<Integer, Float> eIdValueCandidates : x._2().entrySet()) { 
                IntArrayList inNeighborsOfCandidate = inNeighbors_BV.value().get(eIdValueCandidates.getKey());
                if (inNeighborsOfCandidate == null) {
                    continue; //go to next candidate match. this one does not have in-neighbors
                }
                Float tmpNeighborSim = eIdValueCandidates.getValue();
                for (Integer inNeighborOfCandidate : inNeighborsOfCandidate) { //for each in-neighbor of the candidate match of the current entity                    
                    for (Integer eInNeighbor : eInNeighbors) {  //for each in-neighbor of the current entity
                        if (eId < 0) 
                            partialNeighborSims.add(new Tuple2<>(new Tuple2<>(eInNeighbor, inNeighborOfCandidate), tmpNeighborSim));
                        else 
                            partialNeighborSims.add(new Tuple2<>(new Tuple2<>(inNeighborOfCandidate, eInNeighbor), tmpNeighborSim));
                    }
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
            int[] candidates = new int[i];
            while (!x.isEmpty()) {
                candidates[--i] = x.poll().getEntityId(); //get pq elements in reverse (i.e., descending neighbor sim) order
            }
            return new IntArrayList(candidates);
        });
    }
    
    
    
    
    
    public JavaPairRDD<Integer, IntArrayList> getTopKNeighborSimsNEW (JavaPairRDD<Integer,Int2FloatOpenHashMap> valueSims, Broadcast<Map<Integer,IntArrayList>> inNeighbors_BV, int K) {
        return valueSims.flatMapToPair(x->{
            int eId = x._1();
            IntArrayList eInNeighbors = inNeighbors_BV.value().get(eId);
            
            List<Tuple2<Integer,CustomCandidate>> partialNeighborSims = new ArrayList<>(); //key: entityId, value: (candidateId, valueSim(outNeighbor(eId),outNeighbor(cId)) )
            if (eInNeighbors == null) {
                return partialNeighborSims.iterator(); //empty
            }
            for (Map.Entry<Integer, Float> eIdValueCandidates : x._2().entrySet()) { 
                IntArrayList inNeighborsOfCandidate = inNeighbors_BV.value().get(eIdValueCandidates.getKey());
                if (inNeighborsOfCandidate == null) {
                    continue; //go to next candidate match. this one does not have in-neighbors
                }
                Float tmpNeighborSim = eIdValueCandidates.getValue();
                for (Integer inNeighborOfCandidate : inNeighborsOfCandidate) { //for each in-neighbor of the candidate match of the current entity                    
                    for (Integer eInNeighbor : eInNeighbors) {  //for each in-neighbor of the current entity
                        partialNeighborSims.add(new Tuple2<>(eInNeighbor, new CustomCandidate(inNeighborOfCandidate, tmpNeighborSim)));
                        partialNeighborSims.add(new Tuple2<>(inNeighborOfCandidate, new CustomCandidate(eInNeighbor, tmpNeighborSim)));                        
                    }
                }
            }
            
            return partialNeighborSims.iterator();
        })
        .combineByKey(//should be faster than groupByKey (keeps local top-Ks before shuffling, like a combiner in MapReduce)
            //createCombiner
            x-> {
                PriorityQueue<CustomCandidate> initial = new PriorityQueue<>(K);
                initial.add(x);
                return initial; 
            }
            //mergeValue
            , (PriorityQueue<CustomCandidate> pq, CustomCandidate x) -> {
                pq = removeSamePairWithLowerValue(pq, x); //important: fist check that and then add the element
                pq.add(x); //important: only add that after checking for same entity in pq with lower value
                if (pq.size() > K) {
                    pq.poll();
                }
                return pq;
            }
            //mergeCombiners
            , (PriorityQueue<CustomCandidate> pq1, PriorityQueue<CustomCandidate> pq2) -> {
                while (!pq2.isEmpty()) {
                    CustomCandidate c = pq2.poll();
                    pq1 = removeSamePairWithLowerValue(pq1, c); //important: fist check that and then add the element
                    pq1.add(c); //important: only add that after checking for same entity in pq with lower value
                    if (pq1.size() > K) {
                        pq1.poll();
                    }
                }
                return pq1;
            }
        ).mapValues(x -> {      //just reverse the order of candidates and transform values to IntArrayList (topK are kept already)      
            int i = x.size();   
            int[] candidates = new int[i];            
            while (!x.isEmpty()) {
                candidates[--i] = x.poll().getEntityId(); //get pq elements in reverse (i.e., descending neighbor sim) order
            }
            return new IntArrayList(candidates);
        });
    }

    private PriorityQueue<CustomCandidate> removeSamePairWithLowerValue(PriorityQueue<CustomCandidate> pq, CustomCandidate x) {
        int entityIdToAdd = x.getEntityId();
        float valueToAdd = x.getValue();
        CustomCandidate oldElementWithLowerValue = null;
        for (CustomCandidate qElement : pq) { //traverses the queue in random order
            if (qElement.getEntityId() == entityIdToAdd && qElement.getValue() < valueToAdd) { //same entity found with lower value
                oldElementWithLowerValue = qElement;
                break;
            }
        }
        if (oldElementWithLowerValue != null) {
            pq.remove(oldElementWithLowerValue);
        }
        return pq;
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
    
    public CustomCandidate(int entityId, float value) {
        this.entityId = entityId;
        this.value = value;
    }

    @Override
    public int compareTo(CustomCandidate other) {
        // define sorting according to float fields
        return Float.compare(value, other.value); 
    }
    
    public int getEntityId(){
        return entityId;
    }
    
    public float getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return entityId+":"+value;
    }
        
  }        
    
    
}
