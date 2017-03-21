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
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.preprocessing.BlockFiltering;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class RelationsRank {
    
    /**
     * Returns a list of relations sorted in descending score. 
     * @param rawTriples
     * @param SEPARATOR
     * @param minSupportThreshold the minimum support threshold allowed, used for filtering relations with lower support
     * @return a list of relations sorted in descending score.
     */
    public List<String> run(JavaRDD<String> rawTriples, String SEPARATOR, float minSupportThreshold) {        
        Set<String> subjects = getSubjects(rawTriples, SEPARATOR);
        
        JavaPairRDD<String,Iterable<Tuple2<String, String>>> relationIndex = getRelationIndex(rawTriples, SEPARATOR, subjects);        
        
        JavaPairRDD<String,Float> supports = getSupportOfRelations(relationIndex, (long)subjects.size() * subjects.size(), minSupportThreshold);
        JavaPairRDD<String,Float> discrims = getDiscriminabilityOfRelations(relationIndex);
        
        return getSortedRelations(supports, discrims);
    }
    
    public Set<String> getSubjects (JavaRDD<String> rawTriples, String SEPARATOR) {
        return new HashSet<>(rawTriples
                .map(line -> line.split(SEPARATOR)[0])
                .distinct()
                .collect());
    }
    
    public JavaPairRDD<String,Iterable<Tuple2<String, String>>> getRelationIndex(JavaRDD<String> rawTriples, String SEPARATOR, Set<String> subjects) {
        return rawTriples.mapToPair(line -> {
          String[] spo = line.split(SEPARATOR);
          if (spo.length != 3) {
              return null;
          }
          return new Tuple2<>(spo[1], new Tuple2<>(spo[0], spo[2]));
        })
        .filter (x -> {
            if (x == null) {
                return false;
            }
            int relationCount = 0;
            int numInstances = 0;
            for (Tuple2<String,String> so : (Iterable<Tuple2<String,String>>)x._2()) {
                numInstances++;
                if (subjects.contains(so._2())) {
                    relationCount++;
                }
            }
            return relationCount > (numInstances-relationCount); //majority voting (is this property used more as a relation or as a datatype property?
        })
        .groupByKey();        
    }
    
    public JavaPairRDD<String,Float> getSupportOfRelations(JavaPairRDD<String,Iterable<Tuple2<String, String>>> relationIndex, long numEntititiesSquared, float minSupportThreshold) {
        JavaPairRDD<String, Float> unnormalizedSupports = relationIndex
                .mapValues(so -> (float) so.spliterator().getExactSizeIfKnown() / numEntititiesSquared);
        
        float max_support = unnormalizedSupports.values().max(Ordering.natural()); //TODO: check if this is computed correctly without an action        
        return unnormalizedSupports
                .mapValues(x-> x/max_support)
                .filter(x-> x._2()> minSupportThreshold);
    }
    
    public JavaPairRDD<String,Float> getDiscriminabilityOfRelations(JavaPairRDD<String,Iterable<Tuple2<String, String>>> relationIndex) {
        return relationIndex.mapValues(soIterable -> {
                int frequencyOfRelation = 0;
                Set<String> localObjects = new HashSet<>();
                for (Tuple2<String, String> so : soIterable) {
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
    
    
    //TODO: remove this method, keeping int entity ids instead of urls
    public JavaPairRDD<String, Iterable<Tuple2<String, String>>> getRawEntityRelations(JavaRDD<String> rawTriples, List<String> subjects, String SEPARATOR) {
        return rawTriples.mapToPair(line -> {
          String[] spo = line.split(SEPARATOR);
          if (spo.length != 3) {
              return null;
          }
          if (!subjects.contains(spo[2])) { //not a relation
              return null;
          }
          return new Tuple2<>(spo[0], new Tuple2<>(spo[1], spo[2]));
        })
        .filter (x -> x != null)
        .groupByKey();
    }
    
    
    
    /**
     * TODO: Make it possible to return JavaPairRDD<Int, IntArrayList>, replacing entity urls with numeric entity ids (same as in blocking)!!!
     * Get the top-K neighbors (the neighbors found for the top-K relations, based on the local ranking of the relations).
     * @param inputEntities a list of (entityURL, Iterable(attributeName,attributeValue)) pairs
     * @param relationsRank
     * @param K the K from top-K
     * @return 
     */
    public JavaPairRDD<String, String[]> getTopNeighborsPerEntity(JavaPairRDD<String, Iterable<Tuple2<String,String>>> inputEntities, List<String> relationsRank, int K) {
        return inputEntities.mapValues(entityAtts -> {
            PriorityQueue<CustomRelation> topK = new PriorityQueue<>(K);
            for (Tuple2<String,String> att : entityAtts) {
                int relationRank = relationsRank.indexOf(att._1());          
                if (relationRank == -1) { //then this is not a relation
                   continue;
                }  
                CustomRelation curr = new CustomRelation(att._2(), relationRank);
                topK.add(curr);
                if (topK.size() > K) {
                    topK.poll();
                }
            }
            
            String[] result = new String[topK.size()];
            int i = 0;
            while (!topK.isEmpty()) {
                result[i++] = topK.poll().getString();
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
                Logger.getLogger(BlockFiltering.class.getName()).log(Level.SEVERE, null, ex);
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
        final int K = 3;
        
        RelationsRank rr = new RelationsRank();
        JavaRDD<String> rawTriples1 = jsc.textFile(inputPath1);
        JavaRDD<String> rawTriples2 = jsc.textFile(inputPath2);
        
        rawTriples1.persist(StorageLevel.MEMORY_AND_DISK_SER());
        rawTriples2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        List<String> relationsRank1 = rr.run(rawTriples1, SEPARATOR, MIN_SUPPORT_THRESHOLD);
        List<String> relationsRank2 = rr.run(rawTriples2, SEPARATOR, MIN_SUPPORT_THRESHOLD);
        
        
        List<String> subjects1 = null; //a list of (distinct) subject URLs, keeping insertion order (from original triples file)
        List<String> subjects2 = null; //a list of (distinct) subject URLs, keeping insertion order (from original triples file)
        
        JavaPairRDD<String, Iterable<Tuple2<String,String>>> rawEntityRelations1 = rr.getRawEntityRelations(rawTriples1, subjects1, SEPARATOR);
        JavaPairRDD<String, Iterable<Tuple2<String,String>>> rawEntityRelations2 = rr.getRawEntityRelations(rawTriples2, subjects2, SEPARATOR);
        
        
        //TODO: convert  the following results to JavaPairRDD<Integer, IntArrayList>, giving the same entity ids that blocking uses
        JavaPairRDD<String, String[]> topNeighbors1 = rr.getTopNeighborsPerEntity(rawEntityRelations1, relationsRank1, K);
        JavaPairRDD<String, String[]> topNeighbors2 = rr.getTopNeighborsPerEntity(rawEntityRelations2, relationsRank2, K);
        
        
        //TODO: for each entity pair whose top-neighbors share a common block, neighborSim = max value_sim of outNeighbors
        
    }

    
    
/**
 * Copied (and altered) from http://stackoverflow.com/a/16297127/2516301
 */
private static class CustomRelation implements Comparable<CustomRelation>, Serializable {
    // public final fields ok for this small example
    public final String string;
    public double value;

    public CustomRelation(String string, double value) {
        this.string = string;
        this.value = value;
    }

    @Override
    public int compareTo(CustomRelation other) {
        // define sorting according to double fields
        return Double.compare(value, other.value); 
    }
    
    public String getString(){
        return string;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return string+":"+value;
    }
}    
    
}
