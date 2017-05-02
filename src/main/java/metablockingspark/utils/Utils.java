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

package metablockingspark.utils;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class Utils {
    
    public static void deleteHDFSPath(String stringPath) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();        
        FileSystem hdfs = FileSystem.get(new URI(stringPath.substring(0,stringPath.indexOf("/",stringPath.indexOf(":9000")))), conf); //hdfs://clusternode1:9000 or hdfs://master:9000
        Path path = new Path(stringPath);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }
    
    /**
     * Returns the input map sorted by value in descending or ascending order.
     * Adapted from http://stackoverflow.com/a/2581754/2516301
     * @param <K> the key type of the map
     * @param <V> the value type of the map
     * @param map the map to be sorted
     * @param descending sort in descending order?
     * @return a new map, which is the input map sorted by value in descending or ascending order.
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean descending) {
    return map.entrySet()
              .stream()
              .sorted(descending? 
                       Map.Entry.comparingByValue(Collections.reverseOrder())   //descending
                      :Map.Entry.comparingByValue())                            //ascending
              .collect(Collectors.toMap(
                Map.Entry::getKey, 
                Map.Entry::getValue, 
                (e1, e2) -> e1, 
                LinkedHashMap::new
              ));
    }
        
    public static List<String> getEntityUrlsFromEntityRDDInOrder(JavaRDD<String> rawTriples, String SEPARATOR) {
        return new ArrayList<>(
                new LinkedHashSet<>(rawTriples
                .map(line -> encodeURIinUTF8(line.toLowerCase().split(SEPARATOR)[0]))
                .collect())
                ); //convert list to set (to remove duplicates) and back to list (to have index of each element)
    }
    
    /**
     * @deprecated use {@link #readEntityIdsMapping(JavaRDD)} instead, to get the entity mappings used in blocking
     * Maps an entity url to its entity id, that is also used by blocking.
     * @param rawTriples
     * @param SEPARATOR
     * @return a map from an entity url to its entity id, that is also used by blocking.
     */
    public static Object2IntOpenHashMap<String> getEntityIdsMapping(JavaRDD<String> rawTriples, String SEPARATOR) {        
        LinkedHashSet<String> subjectsSet =                  
            new LinkedHashSet<>(rawTriples
            .map(line -> line.split(SEPARATOR)[0])
            .collect()                
            ); //convert list to set (to remove duplicates)
        
        Object2IntOpenHashMap<String> result = new Object2IntOpenHashMap<>(subjectsSet.size());
        result.defaultReturnValue(-1);
        int index = 0;
        for (String subject : subjectsSet) {
            result.put(subject, index++);
        }
        return result;
    }
    
    /**
     * Maps an entity url to its entity id, that is also used by blocking.
     * @param entityIdsText     
     * @param positiveIds false, if the ids will be later converted to negatives, so that their numbering should start from -1, instead of 0
     * @return a map from an entity url to its entity id, that is also used by blocking.
     */
    public static Object2IntOpenHashMap<String> readEntityIdsMapping(JavaRDD<String> entityIdsText, boolean positiveIds) {        
        return new Object2IntOpenHashMap<>(entityIdsText
            .mapToPair(line -> {
                String[] parts = line.toLowerCase().split("\t");
                Integer id = Integer.parseInt(parts[1]);
                return new Tuple2<>(parts[0], positiveIds ? id : id + 1); //negative ids should start from -1, not 0. they will be negated later
            })
            .collectAsMap());
    }
    
    /**
     * @deprecated use {@link #getGroundTruthIdsFromEntityIds(JavaRDD, JavaRDD,JavaRDD, String)} instead
     * Return the ground truth in an RDD format, each entity represented with an integer entity id. 
     * @param rawTriples1
     * @param rawTriples2
     * @param RAW_TRIPLES_SEPARATOR
     * @param gt
     * @param GT_SEPARATOR
     * @return 
     */
    public static JavaPairRDD<Integer,Integer> getGroundTruthIds (JavaRDD<String> rawTriples1, JavaRDD<String> rawTriples2, String RAW_TRIPLES_SEPARATOR, JavaRDD<String> gt, String GT_SEPARATOR) {
        Object2IntOpenHashMap<String> entityIds1 = getEntityIdsMapping(rawTriples1, RAW_TRIPLES_SEPARATOR);
        Object2IntOpenHashMap<String> entityIds2 = getEntityIdsMapping(rawTriples2, RAW_TRIPLES_SEPARATOR); 
        
        return gt.mapToPair(line -> {
                    String [] parts = line.split(GT_SEPARATOR);
                    return new Tuple2<>(-entityIds2.getOrDefault(parts[1], 1), //negative id first
                                        entityIds1.getOrDefault(parts[0], -1)); //positive id second
                });
    }
    
    
    /**
     * Return the ground truth in an RDD format, each entity represented with an integer entity id. 
     * @param entityIds1RDD
     * @param entityIds2RDD
     * @param gt
     * @param GT_SEPARATOR
     * @return 
     */
    public static JavaPairRDD<Integer,Integer> getGroundTruthIdsFromEntityIds (JavaRDD<String> entityIds1RDD, JavaRDD<String> entityIds2RDD, JavaRDD<String> gt, String GT_SEPARATOR) {
        Object2IntOpenHashMap<String> entityIds1 = readEntityIdsMapping(entityIds1RDD, true);
        Object2IntOpenHashMap<String> entityIds2 = readEntityIdsMapping(entityIds2RDD, false); 
        
        return gt.mapToPair(line -> {
                    line = line.toLowerCase();
                    String [] parts = line.split(GT_SEPARATOR);                    
                    parts[1] = encodeURIinUTF8(parts[1]);
                    return new Tuple2<>(-entityIds2.getOrDefault(parts[1], -1), //negative id first (keep default -1, since -(-1) == 1)
                                        entityIds1.getOrDefault(parts[0], -1)); //positive id second
                })
                .filter(x-> x._1() != 1 && x._2() != -1) //throw away pairs whose elements (one or both) do not appear in the dataset
                //remove pairs violating the clean-clean constraint
                .aggregateByKey(new IntOpenHashSet(), 
                        (x,y) -> {x.add(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .filter(x -> x._2().size() == 1) //not more than one match allowed per (negative) entity
                .mapValues(x -> x.iterator().next());
                
    }
    
    /**
     * Return the ground truth in an RDD format, each entity represented with an integer entity id.      
     * @param gt a ground truth file containing matching entities' ids, separated by GT_SEPARATOR
     * @param GT_SEPARATOR
     * @return 
     */
    public static JavaPairRDD<Integer,Integer> readGroundTruthIds (JavaRDD<String> gt, String GT_SEPARATOR) {
        return gt.mapToPair(line -> {                    
                    String [] parts = line.split(GT_SEPARATOR);                    
                    int entity1Id = Integer.parseInt(parts[0]);
                    int entity2Id = Integer.parseInt(parts[1]);
                    return new Tuple2<>(-entity2Id-1, entity1Id);                                        
                });
    }
    
    /**
     * Sets up a new SparkSession
     * @param appName
     * @param parallelismFactor spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
     * @param tmpPath
     * @return 
     */
    public static SparkSession setUpSpark(String appName, int parallelismFactor, String tmpPath) {
        final int NUM_CORES_IN_CLUSTER = 152; //152 in ISL cluster, 28 in okeanos cluster
        final int NUM_WORKERS = 4; //4 in ISL cluster, 14 in okeanos cluster
        final int NUM_EXECUTORS = NUM_WORKERS * 3;
        final int NUM_EXECUTOR_CORES = NUM_CORES_IN_CLUSTER/NUM_EXECUTORS;
        final int PARALLELISM = NUM_EXECUTORS * NUM_EXECUTOR_CORES * parallelismFactor; //spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
                       
        return SparkSession.builder()
            .appName(appName) 
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", PARALLELISM) //x tasks for each core --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "20s")    
                
            .config("spark.executor.instances", NUM_EXECUTORS)
            .config("spark.executor.cores", NUM_EXECUTOR_CORES)
            .config("spark.executor.memory", "55G")
            
            .config("spark.driver.maxResultSize", "2g")
            
            .getOrCreate();        
    }
    
    public static String encodeURIinUTF8(String uri) {
        if (uri.startsWith("<http://dbpedia.org/resource/")) {            
            int splitPoint = uri.lastIndexOf("/")+1;
            String infix = uri.substring(splitPoint, uri.length()-1);
            if (infix.contains("%")) {
                return uri;
            }
            try {
                infix = infix.replace("\\\\", "\\");
                infix = StringEscapeUtils.unescapeJava(infix);
                infix = URLEncoder.encode(infix, "UTF-8");            
            } catch (UnsupportedEncodingException ex) {
                System.err.println("Encoding exception: "+ex);
            }
            uri = uri.substring(0, splitPoint) + infix + ">";            
        }
        return uri;
    }
    
    /**
     * Used in cases where a priority queue has been used to keep top K elements, and then its results are needed in descending order, 
     * in the form of an IntArrayList. The size of the results is equal to the size of the input.
     * @param <T> any subclass of ComparableIntFloatPair
     * @param pq
     * @return 
     */
    public static <T extends ComparableIntFloatPair> IntArrayList toIntArrayListReversed(PriorityQueue<T> pq) {
        int i = pq.size();   
        int[] candidates = new int[i]; 
        while (!pq.isEmpty()) {
            T cand = pq.poll();
            candidates[--i] = cand.getEntityId(); //get pq elements in reverse order
        }
        return new IntArrayList(candidates);
    }
        
}
