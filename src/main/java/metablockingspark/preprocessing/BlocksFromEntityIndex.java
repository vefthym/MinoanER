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

package metablockingspark.preprocessing;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 * Reconstructs a blocking collection from an input entity index.
 * The difference in this output and the initial blocking collection, is that
 * block filtering has been applied.
 * @author vefthym
 */
public class BlocksFromEntityIndex {
    
    public JavaPairRDD<Integer, Iterable<Integer>> run(JavaPairRDD<Integer,Integer[]> entityIndex, LongAccumulator cleanBlocksAccum, LongAccumulator numComparisons) {
        System.out.println("Getting the blocks from the entity index...");
        JavaPairRDD<Integer,Integer> flat = entityIndex.flatMapToPair(x -> 
            {                   
                List<Tuple2<Integer,Integer>> mapResults = new ArrayList<>();
                Integer entityId = x._1();
                for (Integer blockId : x._2()) {
                    mapResults.add(new Tuple2<>(blockId, entityId));
                }
                return mapResults.iterator();
            });
        
        System.out.println("Flattening of entity Index finished. Starting group by block..."); //debugging message
        JavaPairRDD<Integer, Iterable<Integer>> blockGroups = flat.groupByKey();//group by block id       
        
        System.out.println("Group by block finished. Starting filtering singleton blocks and blocks with entities from one collection"); //debugging message
        return blockGroups.filter(x -> { //keep only blocks with > 2 entities and with entities from both datasets (i.e., at least 1 positive and 1 negative entityId)
                Iterable<Integer> entities = (Iterable<Integer>) x._2();
                long negatives = 0;
                boolean containsPositive = false;
                boolean containsNegative = false;
                long numEntities = entities.spliterator().getExactSizeIfKnown();
                if (numEntities < 2) {
                    return false;
                }
                for (int entityId : entities) {
                    if (entityId < 0) {
                        containsNegative = true;
                        negatives++;
                    } else {
                        containsPositive = true;
                    }
                }
                if (containsNegative && containsPositive) {
                    cleanBlocksAccum.add(1);
                    numComparisons.add(negatives * (numEntities-negatives));                
                    return true;
                }
                return false;
            });              
    }
    
}
