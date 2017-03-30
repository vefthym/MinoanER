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

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

/**
 * @deprecated use {@link metablockingspark.entityBased.EntityBasedCNP}
 * @author vefthym
 */
public class EntityBasedCNPSlow implements Serializable {
    
    public JavaPairRDD<Integer,IntArrayList> run(JavaPairRDD<Integer, IntArrayList> blocksFromEI, Broadcast<JavaPairRDD<Integer,Float>> totalWeightsBV, int K, long numNegativeEntities, long numPositiveEntities) {
        
        //map phase
        JavaPairRDD<Integer, IntArrayList> mapOutput = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
        
        JavaPairRDD<Integer,Float> totalWeightsRDD = totalWeightsBV.value();
        //System.out.println(totalWeightsRDD.count()+ " total weights found");
        
        //mapOutput is in the form : <entityId, [neighborIds]>
        JavaPairRDD<Integer,Iterable<IntArrayList>> entitiesBlocks = mapOutput.groupByKey(); //(entityId, Iterable<[neighborIds]>)     //shuffle 1 (or 5)
        //entitiesBlocks is in the form: <entityId, Iterable[neighborIds]>        
        
        JavaPairRDD<Integer,IntArrayList> neighborsRDD; //an array of unique neighbor ids per entityId
        /*neighborsRDD = mapOutput.aggregateByKey(     //shuffle 1
                new HashSet<Integer>(), 
                (x,y) -> {x.addAll(Arrays.asList(y)); return x;}, 
                (x,y) -> {x.addAll(y); return x;}
        ).mapValues(x -> x.toArray(new Integer[x.size()]));        
        */
        // alternative for neighborsRDD
        neighborsRDD = entitiesBlocks.mapToPair(x -> {  //equivalent to mapOutput.aggregateByKey, but uses the cached entitiesBlocks
            IntOpenHashSet neighbors  = new IntOpenHashSet();
            for (IntArrayList candidates : x._2()) {
                neighbors.addAll(candidates);
            }
            return new Tuple2<>(x._1(), new IntArrayList(neighbors.toIntArray()));
        });
        
        
        //the following is very expensive, try to reduce the complexity (try to minimize shuffles)        
        return neighborsRDD.flatMapToPair(x -> {
            List<Tuple2<Integer,Integer>> reversed = new ArrayList<>();
            Integer eId = x._1();
            for (Integer neighbor : x._2()) {
                reversed.add(new Tuple2<>(neighbor, eId));
            }
            reversed.add(new Tuple2<>(eId,eId));
            return reversed.iterator();
        }) // (neighborId, eId)
        .join(totalWeightsRDD) // (neighborId, <eId, weight(neighborId)>        //shuffle 2
        .mapToPair(x -> {
            return new Tuple2<>(x._2()._1(), new Tuple2<>(x._1(), x._2()._2()));
        }) //<eId, <neighborId, weight(neighborId)>>        
        .groupByKey() //<eId, Iterable<neighborId, weight(neighborId)>>     //shuffle 3
        .join(entitiesBlocks) //<eId, (<Iterable<neighborId, weight(neighborId)>, Iterable<[neighborIds]>)>     //shuffle 4   
                
        .mapToPair(x -> {
            Integer entityId = x._1();
            //compute the numerators            
            Int2FloatOpenHashMap counters = new Int2FloatOpenHashMap(); //number of common blocks with current entity per candidate match
            for(IntArrayList candidates : x._2()._2()) { //for each block (as an array of entities)              
                int numNegativeEntitiesInBlock = getNumNegativeEntitiesInBlock(candidates);
                int numPositiveEntitiesInBlock = candidates.size() - numNegativeEntitiesInBlock;
                float weight1 = (float) Math.log10((double)numNegativeEntities/numNegativeEntitiesInBlock);
                float weight2 = (float) Math.log10((double)numPositiveEntities/numPositiveEntitiesInBlock);

                for (int neighborId : candidates) {
                    counters.addTo(neighborId, weight1+weight2);                    
                }
            }
            
            //retrive the totalWeights only for the local entities
            Int2FloatOpenHashMap localEntityWeights = new Int2FloatOpenHashMap();            
            for (Tuple2<Integer, Float> weight : x._2()._1()) {
                localEntityWeights.put(weight._1().intValue(), weight._2().floatValue());
            }            
         
            //calculate the weights of each candidate match (edge in the blocking graph)
            Map<Integer, Float> weights = new HashMap<>();
            float entityWeight = localEntityWeights.get(entityId);
            for (int neighborId : counters.keySet()) {
                float currentWeight = counters.get(neighborId) / (Float.MIN_NORMAL + entityWeight + localEntityWeights.get(neighborId));
                weights.put(neighborId, currentWeight);			
            }

            //keep the top-K weights
            weights = Utils.sortByValue(weights, true);                    
            int[] candidateMatchesSorted = new int[Math.min(weights.size(), K)];                    

            int i = 0;
            for (int neighbor : weights.keySet()) {
                if (i == weights.size() || i == K) {
                    break;
                }
                candidateMatchesSorted[i++] = neighbor;                        
            }
            return new Tuple2<Integer,IntArrayList>(entityId, new IntArrayList(candidateMatchesSorted));
        });               
    }
                
    private int getNumNegativeEntitiesInBlock(Integer[] candidates) {
        int count = 0;
        for (Integer candidate : candidates) {
            if (candidate < 0) {
                count++;
            }
        }
        return count;
    }
    
    private int getNumNegativeEntitiesInBlock(IntArrayList candidates) {        
        return (int) candidates.stream().filter(x -> x<0).count();        
    }
    
}
