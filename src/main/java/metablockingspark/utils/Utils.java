/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metablockingspark.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author vefthym
 */
public class Utils {
    
    public static void deleteHDFSPath(String stringPath) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://master:9000"), conf);
        Path path = new Path(stringPath);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }
    
    /**
     * Copied from http://stackoverflow.com/a/2581754/2516301
     * @param <K>
     * @param <V>
     * @param map
     * @return 
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
    return map.entrySet()
              .stream()
              .sorted(Map.Entry.comparingByValue(Collections.reverseOrder())) //comment out Collections.reverseOrder() to get ascendingOrder
              .collect(Collectors.toMap(
                Map.Entry::getKey, 
                Map.Entry::getValue, 
                (e1, e2) -> e1, 
                LinkedHashMap::new
              ));
    }
}
