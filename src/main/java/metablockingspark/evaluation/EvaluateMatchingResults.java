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
package metablockingspark.evaluation;

import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EvaluateMatchingResults {
    
    /**
     * Compute precision, recall, f-measure of the input results, given the ground truth. 
     * The input RDDs should be in the same format (negative entity Id, positive entity Id). 
     * This is a void method, as it only changes the accumulator values. 
     * @param results the matching results in the form (-entityId, +entityId)
     * @param groundTruth the ground truth in the form (-entityId, +entityId)
     * @param TPs
     * @param FPs
     * @param FNs
     */
    public void evaluateResults(JavaPairRDD<Integer,Integer> results, JavaPairRDD<Integer,Integer> groundTruth, LongAccumulator TPs, LongAccumulator FPs, LongAccumulator FNs) {
        results
                .fullOuterJoin(groundTruth)
                .foreach(joinedMatch -> {
                    Integer myResult = joinedMatch._2()._1().orElse(null);
                    Integer correctResult = joinedMatch._2()._2().orElse(null);
                    if (myResult == null) {
                        FNs.add(1); //missed match
                    } else if (correctResult == null) {
                        FPs.add(1); //wrong match
                    } else if (myResult.equals(correctResult)) {
                        TPs.add(1); //true match
                    } else {        //then I gave a different result than the correct match
                        FPs.add(1); //my result was wrong 
                        FNs.add(1); //the correct match was missed
                    }                    
                });
    }
    
    
    public static void printResults(long tps, long fps, long fns) {
        double precision = (double) tps / (tps + fps);
        double recall = (double) tps / (tps + fns);
        double fMeasure = 2 * (precision * recall) / (precision + recall);
        
        System.out.println("Precision = "+precision+" ("+tps+"/"+(tps+fps)+")");
        System.out.println("Recall = "+recall+" ("+tps+"/"+(tps+fns)+")");
        System.out.println("F-measure = "+ fMeasure);
    }
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String entityIds1, entityIds2;
        String resultsPath, groundTruthPath, groundTruthOutputPath;
        
        
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            entityIds1 = "";
            entityIds2 = "";
            resultsPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";  
            groundTruthPath = "";            
            groundTruthOutputPath = ""; 
        } else if (args.length == 5) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            entityIds1 = args[0];
            entityIds2 = args[1];
            resultsPath = args[2];
            groundTruthPath = args[3];
            groundTruthOutputPath = args[4];
        } else {
            System.out.println("You can run match evaluation with the following arguments:"
                    + "0: entityIds1"
                    + "1: entityIds2"
                    + "2: results path"
                    + "3: ground truth path"
                    + "4: ground truth output path");
            return;
        }
                       
        //tuning params
        final int NUM_CORES_IN_CLUSTER = 152; //152 in ISL cluster, 28 in okeanos cluster
        final int NUM_WORKERS = 4; //4 in ISL cluster, 14 in okeanos cluster
        final int NUM_EXECUTORS = NUM_WORKERS * 3;
        final int NUM_EXECUTOR_CORES = NUM_CORES_IN_CLUSTER/NUM_EXECUTORS;
        final int PARALLELISM = NUM_EXECUTORS * NUM_EXECUTOR_CORES * 2; //spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
                       
        SparkSession spark = SparkSession.builder()
            .appName("Evaluation of "+resultsPath.substring(resultsPath.lastIndexOf("/", resultsPath.length()-2)+1)) 
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", PARALLELISM) //x tasks for each core --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "20s")    
                
            .config("spark.executor.instances", NUM_EXECUTORS)
            .config("spark.executor.cores", NUM_EXECUTOR_CORES)
            .config("spark.executor.memory", "60G")
            //.config("spark.driver.memory", "10g") //not working
            
            //memory configurations (deprecated)
                /*
            .config("spark.memory.useLegacyMode", true)
            .config("spark.shuffle.memoryFraction", 0.4)
            .config("spark.storage.memoryFraction", 0.4) 
            .config("spark.memory.fraction", 0.8)
                */
            //.config("spark.memory.offHeap.enabled", true)
            //.config("spark.memory.offHeap.size", "10g")
                
            .config("spark.driver.maxResultSize", "2g")
            
            .getOrCreate();        
        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());                
        
        
        ////////////////////////
        //start the processing//
        ////////////////////////
                
        System.out.println("Starting the evaluation...");             
        
        String GT_SEPARATOR = ",";
        
        //load the results
        JavaPairRDD<Integer,Integer> matches = jsc.textFile(resultsPath, PARALLELISM)
                .mapToPair(line -> {
                    String[] result = line.substring(1,line.length()-1).split(","); //lose ( and ) and split by comma
                    return new Tuple2<>(Integer.parseInt(result[0]),Integer.parseInt(result[1]));
                });
        
        //Start the evaluation        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");
        EvaluateMatchingResults evaluation = new EvaluateMatchingResults();
        
        JavaPairRDD<Integer,Integer> gt = Utils.getGroundTruthIdsFromEntityIds(jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), jsc.textFile(groundTruthPath), GT_SEPARATOR);
        gt.cache();
        gt.saveAsTextFile(groundTruthOutputPath);
        
        System.out.println("Finished loading the ground truth, now evaluating the results...");   
        
        evaluation.evaluateResults(matches, gt, TPs, FPs, FNs);
        System.out.println("Evaluation finished successfully.");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());                        
        
        spark.stop();
    }
}
