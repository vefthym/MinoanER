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
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class RelationsRank {
    
    /**
     * return a map of topN neighbors per entity
     * @param rawTriples
     * @param SEPARATOR
     * @param MIN_SUPPORT_THRESHOLD
     * @param N topN neighbors per entity
     * @param positiveIds
     * @return 
     */
    public Map<Integer,IntArrayList> run(JavaRDD<String> rawTriples, String SEPARATOR, float MIN_SUPPORT_THRESHOLD, int N, boolean positiveIds, JavaSparkContext jsc) {
        rawTriples.persist(StorageLevel.MEMORY_AND_DISK_SER());        
        
        //List<String> subjects = Utils.getEntityUrlsFromEntityRDDInOrder(rawTriples, SEPARATOR); //a list of (distinct) subject URLs, keeping insertion order (from original triples file)
        Object2IntOpenHashMap<String> subjects = Utils.getEntityIdsMapping(rawTriples, SEPARATOR);
        System.out.println("Found "+subjects.size()+" entities in collection "+ (positiveIds?"1":"2"));
        
        long numEntitiesSquared = (long)subjects.keySet().size();
        
        Broadcast<Object2IntOpenHashMap<String>> subjects_BV = jsc.broadcast(subjects);
        
        JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex = getRelationIndex(rawTriples, SEPARATOR, subjects_BV);        
        
        rawTriples.unpersist();        
        relationIndex.persist(StorageLevel.MEMORY_AND_DISK_SER());        
        
        
        numEntitiesSquared *= numEntitiesSquared;
                        
        List<String> relationsRank = getRelationsRank(relationIndex, MIN_SUPPORT_THRESHOLD, numEntitiesSquared);
        
        Map<Integer, IntArrayList> topNeighbors = getTopNeighborsPerEntity(relationIndex, relationsRank, N, positiveIds).collectAsMap();        
        
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
     * @param subjects
     * @return a relation index of the form: key: relationString, value: list of (subjectId, objectId) linked by this relation
     */
    public JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> getRelationIndex(JavaRDD<String> rawTriples, String SEPARATOR, List<String> subjects) {
        return rawTriples.mapToPair(line -> {
          String[] spo = line.replaceAll(" \\.$", "").split(SEPARATOR);
          if (spo.length != 3) {
              return null;
          }
          Integer subjectId = subjects.indexOf(spo[0]); //replace subject url with entity id (subjects belongs to subjects by default) //TODO: too slow
          Integer objectId = subjects.indexOf(spo[2]); //-1 if the object is not an entity, otherwise the entityId      //TODO: too slow!     
          return new Tuple2<>(spo[1], new Tuple2<>(subjectId, objectId)); //relation, (subjectId, objectId)
        })
        .filter(x -> x!= null)
        .groupByKey()       
        .filter(x -> {
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
                    relationsOnly.add(new Tuple2<>(so._1(), so._2()));
                } 
            }
            return relationsOnly;
        });        
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
        .repartition(512)
        .mapToPair(line -> {
          String[] spo = line.replaceAll(" \\.$", "").split(SEPARATOR);
          if (spo.length != 3) {
              return null;
          }
          int subjectId = subjects_BV.value().getInt(spo[0]); //replace subject url with entity id (subjects belongs to subjects by default)
          int objectId = subjects_BV.value().getInt(spo[2]); //-1 if the object is not an entity, otherwise the entityId      
          return new Tuple2<>(spo[1], new Tuple2<>(subjectId, objectId)); //relation, (subjectId, objectId)
        })
        .setName("rawRelationsRDD")
        .filter(x -> x!= null);
        
        subjects_BV.unpersist();
        subjects_BV.destroy();
        
        return rawRelationsRDD.groupByKey()       
        .filter(x -> {
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
                    relationsOnly.add(new Tuple2<>(so._1(), so._2()));
                } 
            }
            return relationsOnly;
        });        
    }
    
    public JavaPairRDD<String,Float> getSupportOfRelations(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex, long numEntititiesSquared, float minSupportThreshold) {
        JavaPairRDD<String, Float> unnormalizedSupports = relationIndex
                .mapValues(so -> (float) so.spliterator().getExactSizeIfKnown() / numEntititiesSquared);
        unnormalizedSupports.setName("unnormalizedSupports");
        unnormalizedSupports.cache();
        
        System.out.println(unnormalizedSupports.count()+" relations have been assigned a support value"); // dummy action
        float max_support = unnormalizedSupports.values().max(Ordering.natural());
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
     * Get the top-K neighbors (the neighbors found for the top-K relations, based on the local ranking of the relations).
     * @param relationIndex key: relation, value: (subjectId, objectId)
     * @param relationsRank
     * @param K the K from top-K
     * @param postiveIds true if entity ids should be positive, false, if they should be reversed (-eId), i.e., if it is dataset1, or dataset 2
     * @return 
     */
    public JavaPairRDD<Integer, IntArrayList> getTopNeighborsPerEntity(JavaPairRDD<String,Iterable<Tuple2<Integer, Integer>>> relationIndex, List<String> relationsRank, int K, boolean postiveIds) {
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
            .combineByKey( //should be faster than groupByKey (keeps local top-Ks before shuffling, like a combiner in MapReduce)
            //createCombiner
            relation -> {
                PriorityQueue<CustomRelation> initial = new PriorityQueue<>(K);
                int relationRank = relationsRank.indexOf(relation ._1());
                initial.add(new CustomRelation(relation ._2(), relationRank));
                return initial; 
            }
            //mergeValue
            , (PriorityQueue<CustomRelation> pq, Tuple2<String,Integer> relation) -> {
                int relationRank = relationsRank.indexOf(relation._1());
                pq.add(new CustomRelation(relation._2(), relationRank));                
                if (pq.size() > K) {
                    pq.poll();
                }
                return pq;
            }
            //mergeCombiners
            , (PriorityQueue<CustomRelation> pq1, PriorityQueue<CustomRelation> pq2) -> {
                while (!pq2.isEmpty()) {
                    CustomRelation c = pq2.poll();
                    pq1.add(c);
                    if (pq1.size() > K) {
                        pq1.poll();
                    }
                }
                return pq1;
            }
        ).mapValues(topK -> { //just reverse the order of candidates and transform values to IntArrayList (topK are kept already)      
            IntArrayList result = new IntArrayList(topK.size());            
            int i = 0;
            while (!topK.isEmpty()) {                
                result.add(i++, topK.poll().getEntityId());
            }
            return result;
        });
       
    }
    
    
    //only for testing purposes
    public static void main (String[] args) {
        String tmpPath;
        String master;
        String inputPath1, inputPath2;        
        String outputPath;
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputPath1 = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput1";            
            inputPath2 = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput2";            
            outputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else {            
            tmpPath = "/file:/tmp";            
            inputPath1 = args[0];            
            inputPath2 = args[1];            
            outputPath = args[2];
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(outputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(RelationsRank.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                       
        final int NUM_CORES_IN_CLUSTER = 128; //128 in ISL cluster, 28 in okeanos cluster
                       
        SparkSession spark = SparkSession.builder()
            .appName("MetaBlocking WJS on " +
                inputPath1.substring(inputPath1.lastIndexOf("/", inputPath1.length()-2)+1)+" and "+
                inputPath2.substring(inputPath2.lastIndexOf("/", inputPath2.length()-2)+1))
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", NUM_CORES_IN_CLUSTER * 4) //x tasks for each core (128 cores) --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            
            //memory configurations (deprecated)            
            .config("spark.memory.useLegacyMode", true)
            .config("spark.shuffle.memoryFraction", 0.4)
            .config("spark.storage.memoryFraction", 0.4)                
            .config("spark.memory.offHeap.enabled", true)
            .config("spark.memory.offHeap.size", "10g")            

            .getOrCreate();        
        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        
        final String SEPARATOR = " ";
        final float MIN_SUPPORT_THRESHOLD = 0.01f;
        final int N = 3;
        
        JavaRDD<String> rawTriples1 = jsc.textFile(inputPath1);
        JavaRDD<String> rawTriples2 = jsc.textFile(inputPath2);
        
        RelationsRank rr = new RelationsRank();
        Map<Integer, IntArrayList> topNeighbors1 = rr.run(rawTriples1, SEPARATOR, MIN_SUPPORT_THRESHOLD, N, true, jsc);
        Map<Integer, IntArrayList> topNeighbors2 = rr.run(rawTriples2, SEPARATOR, MIN_SUPPORT_THRESHOLD, N, false, jsc);
        
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
        // define sorting according to double fields
        return Double.compare(value, other.value); 
    }
    
    public int getEntityId(){
        return entityId;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return entityId+":"+value;
    }
}    
    
}
