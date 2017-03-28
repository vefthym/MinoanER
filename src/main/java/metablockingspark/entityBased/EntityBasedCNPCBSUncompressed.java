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

package metablockingspark.entityBased;

import java.util.HashMap;
import java.util.Map;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 * Entity based approach for CNP pruning (local top-k) using the CBS (common blocks) weighting scheme. 
 * @author vefthym
 */
public class EntityBasedCNPCBSUncompressed {

    public JavaPairRDD<Integer,Integer[]> run(JavaPairRDD<Integer, IntArrayList> blocksFromEI, int K) {
        
        //map phase
        //resulting RDD is of the form <entityId, [candidateMatchIds]>
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
        
        //reduce phase
        //metaBlockingResults: key: an entityId, value: an array of topK candidate matches, in descending order of score (match likelihood)
        return mapOutput.groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapToPair(x -> {
                    Integer entityId = x._1();
                    
                    //find number of common blocks
                    Map<Integer,Double> counters = new HashMap<>(); //number of common blocks with current entity per candidate match
                    for(IntArrayList neighbors : x._2()) {      //neighbors in the blocking graph           
                        for (int neighborId : neighbors) {                             
                            Double count = counters.get(neighborId);
                            if (count == null) {
                                count = 0.0;
                            }
                            counters.put(neighborId, count+1);
                        }
                    }
                    
                    //keep the top-K weights
                    counters = Utils.sortByValue(counters, true);                    
                    Integer[] candidateMatchesSorted = new Integer[Math.min(counters.size(), K)];                    
                    
                    int i = 0;
                    for (Integer neighbor : counters.keySet()) {
                        if (i == counters.size() || i == K) {
                            break;
                        }
                        candidateMatchesSorted[i++] = neighbor;                        
                    }
                    
                    return new Tuple2<Integer,Integer[]>(entityId, candidateMatchesSorted);
                });
                
    }
    
}
