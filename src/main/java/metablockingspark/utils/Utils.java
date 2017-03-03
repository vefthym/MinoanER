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
