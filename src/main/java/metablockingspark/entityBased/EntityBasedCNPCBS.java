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
public class EntityBasedCNPCBS {

    public JavaPairRDD<Integer,IntArrayList> run(JavaPairRDD<Integer, IntArrayList> blocksFromEI, int K) {
        
        //map phase
        //resulting RDD is of the form <entityId, [candidateMatchIds]>
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
        
        //reduce phase
        //metaBlockingResults: key: an entityId, value: an array of topK candidate matches, in descending order of score (match likelihood)
        return mapOutput.groupByKey() //for each entity create an iterable of arrays of candidate matches (one array from each common block)
                .mapValues(x -> {               
                    //find number of common blocks
                    Map<Integer,Double> counters = new HashMap<>(); //number of common blocks with current entity per candidate match
                    for(IntArrayList block : x) {      //neighbors in the blocking graph           
                        for (int candidateMatch : block) {                                                         
                            counters.put(candidateMatch, counters.getOrDefault(candidateMatch, 0.0)+1);
                        }
                    }
                    
                    //keep the top-K weights
                    counters = Utils.sortByValue(counters, true);                    
                    IntArrayList candidateMatchesSorted = new IntArrayList();
                    
                    int i = 0;
                    for (int candidateMatch : counters.keySet()) {
                        if (i == counters.size() || i == K) {
                            break;
                        }
                        candidateMatchesSorted.add(i++, candidateMatch);                        
                    }
                    
                    return candidateMatchesSorted;
                });
                
    }
    
}
