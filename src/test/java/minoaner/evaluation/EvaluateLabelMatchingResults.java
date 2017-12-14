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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import minoaner.matching.LabelMatchingHeuristic;
import minoaner.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author vefthym
 */
public class EvaluateLabelMatchingResults extends EvaluateMatchingResults {
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputTriples1, inputTriples2, entityIds1, entityIds2;
        String resultsPath, groundTruthPath;
        
        
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputTriples1 = "";
            inputTriples2 = "";
            entityIds1 = "";
            entityIds2 = "";
            resultsPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";  
            groundTruthPath = "";                        
        } else if (args.length == 5) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            inputTriples1 = args[0];
            inputTriples2 = args[1];
            entityIds1 = args[2];
            entityIds2 = args[3];            
            groundTruthPath = args[4];            
        } else {
            System.out.println("You can run match evaluation with the following arguments:"
                    + "0: inputTriples1"
                    + "1: inputTriples2"
                    + "2: entityIds1"
                    + "3: entityIds2"                    
                    + "4: ground truth path");
            return;
        }
                    
        
        String appName = "Evaluation of label matching";
        SparkSession spark = Utils.setUpSpark(appName, 288, 8, 3, tmpPath);
        int PARALLELISM = spark.sparkContext().getConf().getInt("spark.default.parallelism", 144);        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); 
        
        ////////////////////////
        //start the processing//
        ////////////////////////
                
        System.out.println("Starting the evaluation...");             
        
        //YAGO-IMDb
        Set<String> labelAtts1 = new HashSet<>(Arrays.asList("rdfs:label", "label", "skos:prefLabel"));
        Set<String> labelAtts2 = labelAtts1;
        
        String GT_SEPARATOR = ",";
        if (groundTruthPath.contains("music")) {
            GT_SEPARATOR = " ";
            //BBCmusic
            labelAtts1 = new HashSet<>(Arrays.asList("<http://purl.org/dc/elements/1.1/title>", "<http://open.vocab.org/terms/sortLabel>", "<http://xmlns.com/foaf/0.1/name>"));
            labelAtts2 = new HashSet<>(Arrays.asList("<http://www.w3.org/2000/01/rdf-schema#label>", "<http://dbpedia.org/property/name>", "<http://xmlns.com/foaf/0.1/name>"));
        }
        
        if (inputTriples1.contains("rexa")) {
            labelAtts1 = new HashSet<>(Arrays.asList("http://xmlns.com/foaf/0.1/name", "http://www.w3.org/2000/01/rdf-schema#label"));
            labelAtts2 = labelAtts1;
        }
        
        String SEPARATOR = (inputTriples1.endsWith(".tsv"))? "\t" : " ";      
        
        //load the results
        JavaPairRDD<Integer,Integer> matches = new LabelMatchingHeuristic().getMatchesFromLabels(jsc.textFile(inputTriples1, PARALLELISM), jsc.textFile(inputTriples2, PARALLELISM), jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), SEPARATOR, labelAtts1, labelAtts2);
        
        //Start the evaluation        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");
        EvaluateLabelMatchingResults evaluation = new EvaluateLabelMatchingResults();
        
        JavaPairRDD<Integer,Integer> gt;
        if (groundTruthPath.contains("estaurant") || groundTruthPath.contains("Rexa_DBLP")) {
            GT_SEPARATOR = "\t";
            gt = Utils.readGroundTruthIds(jsc.textFile(groundTruthPath), GT_SEPARATOR).cache();
        } else {
            gt = Utils.getGroundTruthIdsFromEntityIds(jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), jsc.textFile(groundTruthPath), GT_SEPARATOR).cache();                    
        }           
        gt.cache();
         
        System.out.println("Finished loading the ground truth with "+ gt.count()+" matches, now evaluating the results...");
        
        evaluation.evaluateResultsNEW(matches, gt, TPs, FPs, FNs);
        System.out.println("Evaluation finished successfully.");
        EvaluateLabelMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());                        
        
        spark.stop();
    }
}
