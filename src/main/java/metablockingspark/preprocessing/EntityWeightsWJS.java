/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metablockingspark.preprocessing;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author vefthym
 */
public class EntityWeightsWJS {
    
    SparkSession spark; 
    JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI;
    JavaPairRDD<Integer,Integer[]> entityIndex;
    Map<Integer,Double> totalWeights;
    final long numPositiveEntities;
    final long numNegativeEntities;
    

    public EntityWeightsWJS(SparkSession spark, JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI, JavaPairRDD<Integer,Integer[]> entityIndex) {
        this.spark = spark;
        this.blocksFromEI = blocksFromEI;
        this.entityIndex = entityIndex;
        totalWeights = new HashMap<>();
        
        JavaRDD<Integer> entityIds = entityIndex.keys().cache();
        
        numNegativeEntities = entityIds.filter(x -> x < 0).count();
        numPositiveEntities = entityIds.count() - numNegativeEntities;
    }

    public Map<Integer, Double> getWeights() {
        if (!totalWeights.isEmpty()) {
            return totalWeights;
        }
        entityIndex.foreach(entityInfo -> {
            int entityId = entityInfo._1();
            Integer[] associatedBlocks = entityInfo._2();
            if (entityId < 0) {
                for (int block : associatedBlocks) {
                    Iterable<Integer> blockContents = blocksFromEI.lookup(block).get(0);
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
                    Iterable<Integer> blockContents = blocksFromEI.lookup(block).get(0);
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
