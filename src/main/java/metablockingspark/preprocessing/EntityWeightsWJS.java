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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author vefthym
 */
public class EntityWeightsWJS implements Serializable {
    
    SparkSession spark;     

    public EntityWeightsWJS (SparkSession spark) {
        this.spark = spark;                       
    }

    public Map<Integer, Double> getWeights(JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, JavaPairRDD<Integer,Integer[]> entityIndex) {
        JavaRDD<Integer> entityIds = entityIndex.keys().cache();
        
        Map<Integer, Iterable<Integer>> blocksMap = blocksFromEI.collectAsMap();
        //JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI = blocksFromEI_BV.value();
        
        long numNegativeEntities = entityIds.filter(x -> x < 0).count();
        long numPositiveEntities = entityIds.count() - numNegativeEntities;
        
        Map<Integer,Double> totalWeights = new HashMap<>();
        entityIndex.foreach(entityInfo -> {
            int entityId = entityInfo._1();
            Integer[] associatedBlocks = entityInfo._2();
            if (entityId < 0) {
                for (int block : associatedBlocks) {
                    //Iterable<Integer> blockContents = blocksFromEI.lookup(block).get(0);
                    Iterable<Integer> blockContents = blocksMap.get(block);
                    int df2t = getNumberOfNegativeEntitiesInBlock(blockContents);
                    double weight2 = Math.log10((double) numNegativeEntities / df2t); //IDF
                    Double totalWeight = totalWeights.get(entityId);
                    if (totalWeight == null) {
                        totalWeight = 0.0;
                    }
                    totalWeight += weight2;
                    totalWeights.put(entityId, totalWeight);
                }
            } else {
                for (int block : associatedBlocks) {
                    //Iterable<Integer> blockContents = blocksFromEI.lookup(block).get(0);
                    Iterable<Integer> blockContents = blocksMap.get(block);
                    int df1t = (int) blockContents.spliterator().getExactSizeIfKnown() - getNumberOfNegativeEntitiesInBlock(blockContents);
                    double weight1 = Math.log10((double) numPositiveEntities / df1t); //IDF
                    Double totalWeight = totalWeights.get(entityId);
                    if (totalWeight == null) {
                        totalWeight = 0.0;
                    }
                    totalWeight += weight1;
                    totalWeights.put(entityId, totalWeight);
                }                
            }
        });
        return totalWeights;
    }
    
    private int getNumberOfNegativeEntitiesInBlock(Iterable<Integer> blockContents) {
        int numNegatives = 0;
        for (Integer entityId : blockContents) {
            if (entityId < 0) {
                numNegatives++;
            }
        }
        return numNegatives;
    }
    
}
