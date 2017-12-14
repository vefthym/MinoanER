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

import it.unimi.dsi.fastutil.ints.Int2FloatLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import minoaner.metablocking.entityBased.neighbors.CNPNeighbors;
import minoaner.metablocking.entityBased.neighbors.CNPARCS;
import minoaner.metablocking.preprocessing.BlockFilteringAdvanced;
import minoaner.metablocking.preprocessing.BlocksFromEntityIndex;
import minoaner.metablocking.preprocessing.EntityWeightsWJS;
import minoaner.utils.Utils;
import minoaner.workflow.FullMetaBlockingWorkflow;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EvaluateBlockingResultsARCS extends BlockingEvaluation {
    
    public static void main(String[] args) {
        String tmpPath;        
        String inputPath;      
        String groundTruthPath, groundTruthOutputPath;        
        String entityIds1, entityIds2;
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";            
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";              
            entityIds1 = "";
            entityIds2 = "";
            groundTruthPath = ""; 
            groundTruthOutputPath = "";
        } else if (args.length >= 4) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            inputPath = args[0];            
            entityIds1 = args[1];
            entityIds2 = args[2];
            groundTruthPath = args[3];
            groundTruthOutputPath = groundTruthPath+"_ids";
            
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(groundTruthOutputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(FullMetaBlockingWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }            
        } else {
            System.out.println("You can run Metablocking with the following arguments:\n"
                    + "0: inputBlocking\n"                    
                    + "1: entityIds1: entityUrl\tentityId (positive)\n"
                    + "2: entityIds2: entityUrl\tentityId (also positive)\n"
                    + "3: ground truth Path\n"
                    + "[4: K]");
            return;
        }
                       
        String appName = "ARCS Blocking evaluation on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1);
        SparkSession spark = Utils.setUpSpark(appName, 288, 8, 3, tmpPath);
        int PARALLELISM = spark.sparkContext().getConf().getInt("spark.default.parallelism", 152);        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); 
        
        
        //Block Filtering
        System.out.println("\n\nStarting BlockFiltering, reading from "+inputPath);
        LongAccumulator BLOCK_ASSIGNMENTS_ACCUM = jsc.sc().longAccumulator();
        BlockFilteringAdvanced bf = new BlockFilteringAdvanced();
        JavaPairRDD<Integer,IntArrayList> entityIndex = bf.run(jsc.textFile(inputPath), BLOCK_ASSIGNMENTS_ACCUM); 
        entityIndex.setName("entityIndex").cache();
        
        
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex...");                
        LongAccumulator CLEAN_BLOCK_ACCUM = jsc.sc().longAccumulator();
        LongAccumulator NUM_COMPARISONS_ACCUM = jsc.sc().longAccumulator();        
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex();
        JavaPairRDD<Integer, IntArrayList> blocksFromEI = bFromEI.run(entityIndex, CLEAN_BLOCK_ACCUM, NUM_COMPARISONS_ACCUM);
        blocksFromEI.setName("blocksFromEI").cache(); //a few hundred MBs        
        
        System.out.println(blocksFromEI.count()+" have been left after block filtering");
        
        double BCin = (double) BLOCK_ASSIGNMENTS_ACCUM.value() / entityIndex.count(); //BCin = average number of block assignments per entity
        final int K = Math.max(1, ((Double)Math.floor(BCin)).intValue()); //K = |_BCin -1_|
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        //CNP        
        System.out.println("Getting the top K value candidates...");
        CNPARCS cnp = new CNPARCS();        
        JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates = cnp.getTopKValueSims(blocksFromEI, K);
        
        blocksFromEI.unpersist();        
        
        
        JavaPairRDD<Integer,IntArrayList> negativeIdResults = topKValueCandidates
                .filter(x-> x._1() < 0)
                .mapValues(x -> new IntArrayList(x.keySet()));
        
        JavaPairRDD<Integer, IntArrayList> positiveIdResults = topKValueCandidates
                .filter(x-> x._1() >= 0)
                .mapValues(x -> new IntArrayList(x.keySet()))                
                .flatMapToPair(x -> { //reverse the candidate pairs
                    List<Tuple2<Integer,Integer>> candidates = new ArrayList<>();
                    for (int candidate : x._2()) {
                        candidates.add(new Tuple2<>(candidate, x._1()));
                    }
                    return candidates.iterator();
                })
                .aggregateByKey(new IntOpenHashSet(), 
                        (x,y) -> {x.add(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .mapValues(x-> new IntArrayList(x));
        
        JavaPairRDD<Integer,IntArrayList> valueResults = 
                negativeIdResults.fullOuterJoin(positiveIdResults)
                .mapValues(x-> {
                    IntArrayList list1 = x._1().orElse(new IntArrayList());
                    IntArrayList list2 = x._2().orElse(new IntArrayList());
                    IntOpenHashSet resultSet = new IntOpenHashSet(list1);
                    resultSet.addAll(list2);
                    return new IntArrayList(resultSet);
                });
        
        
        blocksFromEI.unpersist();    
        
        
        
        //Start the evaluation        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");
        EvaluateBlockingResultsARCS evaluation = new EvaluateBlockingResultsARCS();
        
        String GT_SEPARATOR = ",";
        if (groundTruthPath.contains("music")) {
            GT_SEPARATOR = " ";
        }
        
        JavaPairRDD<Integer,Integer> gt = Utils.getGroundTruthIdsFromEntityIds(jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), jsc.textFile(groundTruthPath), GT_SEPARATOR);
        gt.cache();
        gt.saveAsTextFile(groundTruthOutputPath);
        
        System.out.println("Finished loading the ground truth with "+ gt.count()+" matches, now evaluating the results...");  
        
        evaluation.evaluateBlockingResults(valueResults, gt, TPs, FPs, FNs, false);
        System.out.println("Evaluation finished successfully.");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());   
        
        spark.stop();
    }
    
    
    
    
}
