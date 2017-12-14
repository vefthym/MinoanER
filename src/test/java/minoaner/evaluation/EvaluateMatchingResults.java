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
package minoaner.evaluation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import minoaner.utils.Utils;
import minoaner.workflow.FullMetaBlockingWorkflow;
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
    public void evaluateResultsNEW(JavaPairRDD<Integer,Integer> results, JavaPairRDD<Integer,Integer> groundTruth, LongAccumulator TPs, LongAccumulator FPs, LongAccumulator FNs) {
        results
                .rightOuterJoin(groundTruth)
                .foreach(joinedMatch -> {
                    Integer myResult = joinedMatch._2()._1().orElse(null);
                    Integer correctResult = joinedMatch._2()._2();
                    if (myResult == null) {
                        FNs.add(1); //missed match
                    /*} else if (correctResult == null) {
                        FPs.add(1); //wrong match
                        */
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
        } else if (args.length == 4) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            entityIds1 = args[0];
            entityIds2 = args[1];
            resultsPath = args[2];
            groundTruthPath = args[3];
            groundTruthOutputPath = groundTruthPath+"_ids";
            
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(groundTruthOutputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(FullMetaBlockingWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            System.out.println("You can run match evaluation with the following arguments:"
                    + "0: entityIds1"
                    + "1: entityIds2"
                    + "2: matching results path"
                    + "3: ground truth path");
            return;
        }
                    
        
        String appName = "Evaluation of "+resultsPath.substring(resultsPath.lastIndexOf("/", resultsPath.length()-2)+1);
        SparkSession spark = Utils.setUpSpark(appName, 288, 8, 3, tmpPath);
        int PARALLELISM = spark.sparkContext().getConf().getInt("spark.default.parallelism", 152);        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); 
        
        ////////////////////////
        //start the processing//
        ////////////////////////
                
        System.out.println("Starting the evaluation...");             
        
        String GT_SEPARATOR = ",";
        if (groundTruthPath.contains("music")) {
            GT_SEPARATOR = " ";
        }
        
        //load the results
        JavaPairRDD<Integer,Integer> matches = jsc.textFile(resultsPath, PARALLELISM)
                .mapToPair(line -> {
                    String[] result = line.substring(1,line.length()-1).split(","); //lose '(' and ')' and split by comma
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
        
        System.out.println("Finished loading the ground truth with "+ gt.count()+" matches, now evaluating the results...");
        
        evaluation.evaluateResults(matches, gt, TPs, FPs, FNs);
        System.out.println("Evaluation finished successfully.");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());                        
        
        spark.stop();
    }
}
