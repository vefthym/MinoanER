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
    
    /**
     * Get for each entity a block a tuple like eId, [entitiesFromTheOtherCollectionInThisBlock[
     * @param blocksFromEI the blocks after block filtering, in the form: blockId, [entityIds]
     * @return for each entity in a block a tuple like eId, [entitiesFromTheOtherCollectionInThisBlock[
     */
    public static JavaPairRDD<Integer,IntArrayList> getMapOutput(JavaPairRDD<Integer, IntArrayList> blocksFromEI) {        
        return blocksFromEI.flatMapToPair(block -> {            
            IntArrayList positives = new IntArrayList();
            IntArrayList negatives = new IntArrayList();
            	
            for (int entityId : block._2()) { 
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
            
            //emit all the negative entities array for each positive entity
            for (int positiveId : positives) {
                mapResults.add(new Tuple2<>(positiveId, negatives));                
            }

            //emit all the positive entities array for each negative entity
            for (int negativeId : negatives) {
                mapResults.add(new Tuple2<>(negativeId, positives));                
            }
            
            return mapResults.iterator();
        })
        .filter(x-> x != null); //comment out when return null is replaced by return new ArrayList<>().iterator()
    }
    
    
    
    
    /**
     * Get for each entity a block a tuple like eId, [entitiesFromTheOtherCollectionInThisBlock].
     * The first element in the values is the number of entities from the same collection, which will be later used to calculate WJS. 
     * @param blocksFromEI the blocks after block filtering, in the form: blockId, [entityIds]
     * @return for each entity in a block a tuple like eId, [numEntitiesFromTheSameCollection,entitiesFromTheOtherCollectionInThisBlock[
     */
    public static JavaPairRDD<Integer,IntArrayList> getMapOutputWJS(JavaPairRDD<Integer, IntArrayList> blocksFromEI) {        
        return blocksFromEI.flatMapToPair(block -> {            
            IntArrayList positives = new IntArrayList();
            IntArrayList negatives = new IntArrayList();
            	
            for (int entityId : block._2()) { 
                if (entityId < 0) {
                    negatives.add(entityId);
                } else {
                    positives.add(entityId);
                }
            }
            
            List<Tuple2<Integer,IntArrayList>> mapResults = new ArrayList<>();
            
            if (positives.isEmpty() || negatives.isEmpty()) {                
                return null;                
            }        
            
            //add as a first element to negatives the number of positives
            IntArrayList negativesToEmit = new IntArrayList();            
            negativesToEmit.add(0, positives.size());
            negativesToEmit.addAll(negatives);
            
            //add as a first element to positives the number of negatives
            IntArrayList positivesToEmit = new IntArrayList();
            positivesToEmit.add(0, negatives.size());
            positivesToEmit.addAll(positives);
            
            
            //emit all the negative entities array for each positive entity
            for (int positiveId : positives) {
                mapResults.add(new Tuple2<>(positiveId, negativesToEmit));                
            }

            //emit all the positive entities array for each negative entity
            for (int negativeId : negatives) {
                mapResults.add(new Tuple2<>(negativeId, positivesToEmit));                
            }
            
            return mapResults.iterator();
        })
        .filter(x-> x != null && x._2().size() > 1); //why does the second condition not work?
    }
}
