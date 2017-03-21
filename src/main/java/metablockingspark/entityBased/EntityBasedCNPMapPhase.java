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

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPMapPhase {
    
    public static JavaPairRDD<Integer,IntArrayList> getMapOutput(JavaPairRDD<Integer, IntArrayList> blocksFromEI) {        
        return blocksFromEI.flatMapToPair(x -> {            
            List<Integer> positives = new ArrayList<>();
            List<Integer> negatives = new ArrayList<>();
		
            for (int entityId : x._2()) { 
                if (entityId < 0) {
                    negatives.add(entityId);
                } else {
                    positives.add(entityId);
                }
            }
            
            List<Tuple2<Integer,IntArrayList>> mapResults = new ArrayList<>();
            
            if (positives.isEmpty() || negatives.isEmpty()) {                
                return null;
                //return mapResults.iterator(); //empty result on purpose (to avoid returning null and then filtering out null results)
            }
            
            int[] positivesArray = positives.stream().mapToInt(i->i).toArray();
            int[] negativesArray = negatives.stream().mapToInt(i->i).toArray();
            
            IntArrayList positivesToEmit = new IntArrayList(positivesArray);
            IntArrayList negativesToEmit = new IntArrayList(negativesArray);                        
            
            //emit all the negative entities array for each positive entity
            for (int i = 0; i < positivesArray.length; ++i) {                
                mapResults.add(new Tuple2<>(positivesArray[i], negativesToEmit));                
            }

            //emit all the positive entities array for each negative entity
            for (int i = 0; i < negativesArray.length; ++i) {
                mapResults.add(new Tuple2<>(negativesArray[i], positivesToEmit));                
            }
            
            return mapResults.iterator();
        })
        .filter(x-> x != null); //comment out when return null is replaced by return new ArrayList<>().iterator()
    }
}
