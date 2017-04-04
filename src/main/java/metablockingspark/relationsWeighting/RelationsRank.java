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
package metablockingspark.relationsWeighting;

import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class RelationsRank implements Serializable {
    
    /**
     * return a map of topN neighbors per entity
     * @param rawTriples
     * @param SEPARATOR
     * @param entityIdsRDD
     * @param MIN_SUPPORT_THRESHOLD
     * @param N topN neighbors per entity
     * @param positiveIds
     * @param jsc
     * @return 
     */
    public Map<Integer,IntArrayList> run(JavaRDD<String> rawTriples, String SEPARATOR, JavaRDD<String> entityIdsRDD, float MIN_SUPPORT_THRESHOLD, int N, boolean positiveIds, JavaSparkContext jsc) {
        rawTriples.persist(StorageLevel.MEMORY_AND_DISK_SER());        
        
        //List<String> subjects = Utils.getEntityUrlsFromEntityRDDInOrder(rawTriples, SEPARATOR); //a list of (distinct) subject URLs, keeping insertion order (from original triples file)        
        //Object2IntOpenHashMap<String> subjects = Utils.getEntityIdsMapping(rawTriples, SEPARATOR);
        Object2IntOpenHashMap<String> entityIds = Utils.readEntityIdsMapping(entityIdsRDD);
        System.out.println("Found "+entityIds.size()+" entities in collection "+ (positiveIds?"1":"2"));
        
        long numEntitiesSquared = (long)entityIds.keySet().size();
        numEntitiesSquared *= numEntitiesSquared;
        
        Broadcast<Object2IntOpenHashMap<String>> entityIds_BV = jsc.broadcast(entityIds);
        
        JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex = getRelationIndex(rawTriples, SEPARATOR, entityIds_BV);        
        
        rawTriples.unpersist();        
        relationIndex.persist(StorageLevel.MEMORY_AND_DISK_SER());        
        
                        
        List<String> relationsRank = getRelationsRank(relationIndex, MIN_SUPPORT_THRESHOLD, numEntitiesSquared);
        //System.out.println("Sorted relations:"+Arrays.toString(relationsRank.toArray()));
        
        Map<Integer, IntArrayList> topNeighbors = getTopNNeighborsPerEntity(relationIndex, relationsRank, N, positiveIds).collectAsMap(); //action
        
        int i = 10;
        for (Map.Entry<Integer, IntArrayList> entry : topNeighbors.entrySet()) {
            System.out.println("top neighbors of "+entry.getKey()+":"+entry.getValue().toString());            
            if (--i == 0) {
                break;
            }
        }        
        
        relationIndex.unpersist(); 
        
        return topNeighbors;
    }
    
    
    
    /**
     * Returns a list of relations sorted in descending score.      
     * @param relationIndex
     * @param minSupportThreshold the minimum support threshold allowed, used for filtering relations with lower support
     * @param numEntitiesSquared
     * @return a list of relations sorted in descending score.
     */
    public List<String> getRelationsRank(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex, float minSupportThreshold, long numEntitiesSquared) {                        
        JavaPairRDD<String,Float> supports = getSupportOfRelations(relationIndex, numEntitiesSquared, minSupportThreshold);
        JavaPairRDD<String,Float> discrims = getDiscriminabilityOfRelations(relationIndex);
        
        return getSortedRelations(supports, discrims);
    }   
    
    /**
     * Returns a relation index of the form: key: relationString, value: list of (subjectId, objectId) linked by this relation.
     * @param rawTriples
     * @param SEPARATOR
     * @param subjects_BV
     * @return a relation index of the form: key: relationString, value: list of (subjectId, objectId) linked by this relation
     */
    public JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> getRelationIndex(JavaRDD<String> rawTriples, String SEPARATOR, Broadcast<Object2IntOpenHashMap<String>> subjects_BV) {        
        JavaPairRDD<String,Tuple2<Integer,Integer>> rawRelationsRDD = rawTriples        
        .mapToPair(line -> {
          String[] spo = line.replaceAll(" \\.$", "").split(SEPARATOR);
          if (spo.length != 3) {
              return null;
          }
          int subjectId = subjects_BV.value().getInt(spo[0]); //replace subject url with entity id (subjects belongs to subjects by default)
          int objectId = subjects_BV.value().getOrDefault(spo[2], -1); //-1 if the object is not an entity, otherwise the entityId      
          return new Tuple2<>(spo[1], new Tuple2<>(subjectId, objectId)); //relation, (subjectId, objectId)
        })        
        .filter(x -> x!= null);
        
        //subjects_BV.unpersist();
        //subjects_BV.destroy();
        
        return rawRelationsRDD.groupByKey()       
        .filter(x -> {                  //keep only relations (properties that have more object values than datatype values)
            int relationCount = 0;
            int numInstances = 0;
            for (Tuple2<Integer,Integer> so : x._2()) {
                numInstances++;
                if (so._2() != -1) {
                    relationCount++;
                }
            }
            return relationCount > (numInstances-relationCount); //majority voting (is this property used more as a relation or as a datatype property?
        })
        .mapValues(x -> {
            List<Tuple2<Integer,Integer>> relationsOnly = new ArrayList<>();
            for (Tuple2<Integer,Integer> so : x) {                
                if (so._2() != -1) {
                    relationsOnly.add(new Tuple2<>(so._1(), so._2())); //(subject, object) pairs connected with this relation
                } 
            }
            return relationsOnly;
        });        
    }
    
    public JavaPairRDD<String,Float> getSupportOfRelations(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex, long numEntititiesSquared, float minSupportThreshold) {
        JavaPairRDD<String, Float> unnormalizedSupports = relationIndex
                .mapValues(so -> (float) so.spliterator().getExactSizeIfKnown() / numEntititiesSquared);
        unnormalizedSupports.setName("unnormalizedSupports").cache();        
        
        System.out.println(unnormalizedSupports.count()+" relations have been assigned a support value"); // dummy action to trigger execution
        float max_support = unnormalizedSupports.values().max(Ordering.natural());
        System.out.println("Max support = "+max_support);
        return unnormalizedSupports
                .mapValues(x-> x/max_support)
                .filter(x-> x._2()> minSupportThreshold);
    }
    
    public JavaPairRDD<String,Float> getDiscriminabilityOfRelations(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex) {
        return relationIndex.mapValues(soIterable -> {
                int frequencyOfRelation = 0;
                IntOpenHashSet localObjects = new IntOpenHashSet();
                for (Tuple2<Integer, Integer> so : soIterable) {
                    frequencyOfRelation++;
                    localObjects.add(so._2());
                }
                return (float) localObjects.size() / frequencyOfRelation;
            });               
    }
    
    public List<String> getSortedRelations(JavaPairRDD<String,Float> supports, JavaPairRDD<String,Float> discriminabilities) {
        return supports
                .join(discriminabilities)
                .mapValues(x-> (2* x._1() * x._2()) / (x._1() + x._2())) // keep the f-measure of support and discriminability as the score of a relation
                .mapToPair(x-> x.swap()) //key: score, value: relation name
                .sortByKey(false)       //sort relations in descedning score
                .values()               //get the sorted (by score) relation names
                .collect();        
    }
    
    /**
     * Get the top-N neighbors (the neighbors found for the top-K relations, based on the local ranking of the relations).
     * @param relationIndex key: relation, value: (subjectId, objectId)
     * @param relationsRank a global ranking of relations per dataset (the rank of each relation is its index in this list, starting from 0 for top-ranked)
     * @param N the N from top-N
     * @param postiveIds true if entity ids should be positive, false, if they should be reversed (-eId), i.e., if it is dataset1, or dataset 2
     * @return 
     */
    public JavaPairRDD<Integer, IntArrayList> getTopNNeighborsPerEntity(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex, List<String> relationsRank, int N, boolean postiveIds) {
        return relationIndex.flatMapToPair(x-> {
                List<Tuple2<Integer, Tuple2<String, Integer>>> entities = new ArrayList<>(); //key: subjectId, value: (relation, objectId)
                for (Tuple2<Integer,Integer> relatedEntities : x._2()) {
                    if (postiveIds) {
                        entities.add(new Tuple2<>(relatedEntities._1(), new Tuple2<>(x._1(), relatedEntities._2())));
                    } else {
                        entities.add(new Tuple2<>(-relatedEntities._1(), new Tuple2<>(x._1(), -relatedEntities._2())));
                    }
                }
                return entities.iterator();
            })                   
            .combineByKey( //should be faster than groupByKey (keeps local top-Ns before shuffling, like a combiner in MapReduce)
            //createCombiner
            relation -> {
                PriorityQueue<CustomRelation> initial = new PriorityQueue<>();
                int relationRank = relationsRank.indexOf(relation._1());
                initial.add(new CustomRelation(relation._2(), relationRank)); //relation's name, relation's rank
                return initial; 
            }
            //mergeValue
            , (PriorityQueue<CustomRelation> pq, Tuple2<String,Integer> relation) -> {
                int relationRank = relationsRank.indexOf(relation._1());
                CustomRelation c = new CustomRelation(relation._2(), relationRank);
                pq.add(c);         
                pq = removeSameNeighborWithLowerRank(pq, c); //from duplicate neighbor Ids, keep the one from the relation with the better ranking                                                       
                if (pq.size() > N) {
                    pq.poll();
                }
                return pq;
            }
            //mergeCombiners
            , (PriorityQueue<CustomRelation> pq1, PriorityQueue<CustomRelation> pq2) -> {
                while (!pq2.isEmpty()) {
                    CustomRelation c = pq2.poll();                    
                    pq1.add(c);
                    pq1 = removeSameNeighborWithLowerRank(pq1, c);
                    if (pq1.size() > N) {
                        pq1.poll();
                    }
                }
                return pq1;
            }
        ).mapValues(topN -> { //just reverse the order of candidates and transform values to IntArrayList (topN are kept already)                  
            int i = topN.size();   
            int[] candidates = new int[i];            
            while (!topN.isEmpty()) {
                candidates[--i] = topN.poll().getEntityId(); //get pq elements in reverse (i.e., ascending rank) order                
            }            
            return new IntArrayList(candidates);
        });
       
    }
        
    /**
     * At this point, pq contains x and maybe 1 more element y with the same key as x. If y exists, keep from those two the one with the better value. 
     * If y does not exist, keep x. 
     * @param pq
     * @param x
     * @return 
     */
    private PriorityQueue<CustomRelation> removeSameNeighborWithLowerRank(PriorityQueue<CustomRelation> pq, CustomRelation x) {
        int neighborIdToAdd = x.getEntityId();
        double newValue = x.getValue();
        CustomRelation elementToDelete = null;
        boolean sameRankTwice = false;
        for (CustomRelation qElement : pq) { //traverses the queue in random order
            if (qElement.getEntityId() == neighborIdToAdd) {
                if (qElement.getValue() > newValue) { //y is worse than x => delete y
                    elementToDelete = qElement;
                    break;
                } else if (qElement.getValue() < newValue) { //y is better than x => delete x
                    elementToDelete = x;
                    break;
                } else {  //qElement has the same value as x
                    if (!sameRankTwice) { //first time meeting this element (it can be x or a y with the same rank)
                        sameRankTwice = true; 
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
    
    
/**
 * Copied (and altered) from http://stackoverflow.com/a/16297127/2516301
 */
private static class CustomRelation implements Comparable<CustomRelation>, Serializable {
    // public final fields ok for this small example
    public final int entityId;
    public double value;

    public CustomRelation(int entityId, double value) {
        this.entityId = entityId;
        this.value = value;
    }

    @Override
    public int compareTo(CustomRelation other) {
        // define (descending) sorting according to double fields
        return -Double.compare(value, other.value); 
    }
    
    public int getEntityId(){
        return entityId;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    public double getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return entityId+":"+value;
    }
}    
    
}
