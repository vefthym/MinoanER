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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 * Reconstructs a blocking collection from an input entity index.
 * The difference in this output and the initial blocking collection, is that
 * block filtering has been applied.
 * @author vefthym
 */
public class BlocksFromEntityIndex {
    
    SparkSession spark;
    String blocksFromEIOutputPath;
    JavaPairRDD<Integer,Integer[]> entityIndex;    

    public BlocksFromEntityIndex(SparkSession spark, JavaPairRDD<Integer,Integer[]> entityIndex, String blocksFromEIOutputPath) {
        this.spark = spark;        
        this.entityIndex = entityIndex;
        this.blocksFromEIOutputPath = blocksFromEIOutputPath;        
    }
    
    public BlocksFromEntityIndex(SparkSession spark, String entityIndexInputPath, String blocksFromEIOutputPath) {
        this(spark, 
             JavaPairRDD.fromJavaRDD(JavaSparkContext.fromSparkContext(spark.sparkContext()).objectFile(entityIndexInputPath)), 
             blocksFromEIOutputPath);
    }
    
    public BlocksFromEntityIndex(SparkSession spark, JavaPairRDD<Integer,Integer[]> entityIndex) {
        this(spark, entityIndex, null);
    }
    
    public JavaPairRDD<Integer, Iterable<Integer>> run(LongAccumulator cleanBlocksAccum, LongAccumulator numComparisons) {
        return entityIndex.flatMapToPair(x -> 
            {
                List<Tuple2<Integer,Integer>> mapResults = new ArrayList<>();
                for (int blockId : x._2()) {
                    mapResults.add(new Tuple2<>(blockId, x._1()));
                }
                return mapResults.iterator();
            })
            .groupByKey()            
            .filter(x -> { //keep only blocks with > 2 entities and with entities from both datasets (i.e., at least 1 positive and 1 negative entityId)
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
