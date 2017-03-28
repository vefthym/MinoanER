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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import metablockingspark.entityBased.neighbors.EntityBasedCNPNeighborsInMemory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaRDD;

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
                .map(line -> line.split(SEPARATOR)[0])
                .collect())
                ); //convert list to set (to remove duplicates) and back to list (to have index of each element)
    }
    
    /**
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
}
