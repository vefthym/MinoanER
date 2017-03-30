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

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.entityBased.EntityBasedCNPCBS;
import metablockingspark.matching.ReciprocalMatching;
import metablockingspark.entityBased.neighbors.EntityBasedCNPNeighbors;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.preprocessing.EntityWeightsWJS;
import metablockingspark.rankAggregation.LocalRankAggregation;
import metablockingspark.utils.Utils;
import metablockingspark.workflow.FullMetaBlockingWorkflow;
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
public class EvaluateBlockingResults {
    
    /**
     * Compute precision, recall, f-measure of the input results, given the ground truth. 
     * The input RDDs should be in the same format (negative entity Id, positive entity Id).
     * @param blockingResults the blocking results in the form (-entityId, +entityId)
     * @param groundTruth the ground truth in the form (-entityId, +entityId)
     * @param TPs
     * @param FPs
     * @param FNs
     */
    public void evaluateBlockingResults(JavaPairRDD<Integer,IntArrayList> blockingResults, JavaPairRDD<Integer,Integer> groundTruth, LongAccumulator TPs, LongAccumulator FPs, LongAccumulator FNs) {
        blockingResults
                .fullOuterJoin(groundTruth)
                .foreach(joinedMatch -> {
                    IntArrayList myCandidates = joinedMatch._2()._1().orElse(null);
                    Integer correctResult = joinedMatch._2()._2().orElse(null);
                    if (myCandidates == null) { //this means that the correct result is not null (otherwise, nothing to join here)
                        FNs.add(1); //missed match
                    } else if (correctResult == null) {
                        FPs.add(myCandidates.size()); //each candidate is a false match (no candidate should exist)
                    } else if (myCandidates.contains(correctResult)) {
                        TPs.add(1); //true match
                        FPs.add(myCandidates.size()-1); //the rest are false matches (ideal: only one candidate suggested)
                    } else {        //then the correct result is not included in my candidates => I missed this match and all my candidates are wrong
                        FPs.add(myCandidates.size()); //all my candidates were wrong 
                        FNs.add(1); //the correct match was missed
                    }                    
                });
    }
    
    
    
    public static void main(String[] args) {
        String tmpPath;        
        String inputPath;      
        String groundTruthPath, groundTruthOutputPath;
        String inputTriples1, inputTriples2;
        String entityIds1, entityIds2;
        
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";            
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";  
            inputTriples1 = "";
            inputTriples2 = "";
            entityIds1 = "";
            entityIds2 = "";
            groundTruthPath = "";
            groundTruthOutputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else if (args.length == 7) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            inputPath = args[0];
            inputTriples1 = args[1];
            inputTriples2 = args[2];
            entityIds1 = args[3];
            entityIds2 = args[4];
            groundTruthPath = args[5];
            groundTruthOutputPath = args[6];
            
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(groundTruthOutputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(FullMetaBlockingWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            System.out.println("You can run Metablocking with the following arguments:\n"
                    + "0: inputBlocking\n"
                    + "1: inputTriples1 (raw rdf triples)\n"
                    + "2: inputTriples2 (raw rdf triples)\n"
                    + "3: entityIds1: entityUrl\tentityId (positive)\n"
                    + "4: entityIds2: entityUrl\tentityId (also positive)\n"
                    + "5: ground truth Path\n"
                    + "6: ground truth output oath\n");
            return;
        }
                       
        //tuning params
        final int NUM_CORES_IN_CLUSTER = 152; //152 in ISL cluster, 28 in okeanos cluster
        final int NUM_WORKERS = 4; //4 in ISL cluster, 14 in okeanos cluster
        final int NUM_EXECUTORS = NUM_WORKERS * 3;
        final int NUM_EXECUTOR_CORES = NUM_CORES_IN_CLUSTER/NUM_EXECUTORS;
        final int PARALLELISM = NUM_EXECUTORS * NUM_EXECUTOR_CORES * 3; //spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
                       
        SparkSession spark = SparkSession.builder()
            .appName("Blocking evaluation on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1)) 
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", PARALLELISM) //x tasks for each core --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "20s")    
                
            .config("spark.executor.instances", NUM_EXECUTORS)
            .config("spark.executor.cores", NUM_EXECUTOR_CORES)
            .config("spark.executor.memory", "60G")
            
            .config("spark.driver.maxResultSize", "2g")
            
            .getOrCreate();        
        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());        
        LongAccumulator BLOCK_ASSIGNMENTS_ACCUM = jsc.sc().longAccumulator();
        
        
        ////////////////////////
        //start the processing//
        ////////////////////////
        
        //Block Filtering
        System.out.println("\n\nStarting BlockFiltering, reading from "+inputPath);
        
        BlockFilteringAdvanced bf = new BlockFilteringAdvanced();
        JavaPairRDD<Integer,IntArrayList> entityIndex = bf.run(jsc.textFile(inputPath), BLOCK_ASSIGNMENTS_ACCUM); 
        entityIndex.setName("entityIndex");
        entityIndex.cache();
        //entityIndex.persist(StorageLevel.DISK_ONLY_2()); //store to disk with replication factor 2
        
        
        //long numEntities = entityIndex.keys().count();
        
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex...");
                
        LongAccumulator CLEAN_BLOCK_ACCUM = jsc.sc().longAccumulator();
        LongAccumulator NUM_COMPARISONS_ACCUM = jsc.sc().longAccumulator();
        
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex();
        JavaPairRDD<Integer, IntArrayList> blocksFromEI = bFromEI.run(entityIndex, CLEAN_BLOCK_ACCUM, NUM_COMPARISONS_ACCUM);
        blocksFromEI.setName("blocksFromEI");
        //blocksFromEI.persist(StorageLevel.DISK_ONLY());
        blocksFromEI.cache(); //a few hundreds MBs
        
        
        //get the total weights of each entity, required by WJS weigthing scheme (only)
        /*
        System.out.println("\n\nStarting EntityWeightsWJS...");
        EntityWeightsWJS wjsWeights = new EntityWeightsWJS();        
        Int2FloatOpenHashMap totalWeights = new Int2FloatOpenHashMap(); //saves memory by storing data as primitive types        
        wjsWeights.getWeights(blocksFromEI, entityIndex).foreach(entry -> {
            totalWeights.put(entry._1().intValue(), entry._2().floatValue());
        });
        Broadcast<Int2FloatOpenHashMap> totalWeights_BV = jsc.broadcast(totalWeights);        
        */
        
        blocksFromEI.count(); //dummy action (only in CBS)
        long numEntities = entityIndex.count();
        
        double BCin = (double) BLOCK_ASSIGNMENTS_ACCUM.value() / numEntities; //BCin = average number of block assignments per entity
        final int K = ((Double)Math.floor(BCin - 1)).intValue(); //K = |_BCin -1_|
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        /*
        long numNegativeEntities = wjsWeights.getNumNegativeEntities();
        long numPositiveEntities = wjsWeights.getNumPositiveEntities();
        
        System.out.println("Found "+numNegativeEntities+" negative entities");
        System.out.println("Found "+numPositiveEntities+" positive entities");
        */
        
        //CNP
        System.out.println("\n\nStarting CNP...");        
        
        //CBS
        EntityBasedCNPCBS cnp = new EntityBasedCNPCBS();
        JavaPairRDD<Integer,IntArrayList> valueResults = cnp.run(blocksFromEI, K);
        //end of CBS
        
        //WJS
        final float MIN_SUPPORT_THRESHOLD = 0.01f;
        final int N = 3; //for top-N neighbors
        
        System.out.println("Getting the top K value candidates...");
        /*EntityBasedCNPNeighbors cnp = new EntityBasedCNPNeighbors();        
        JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates = cnp.getTopKValueSims(blocksFromEI, totalWeights_BV, K, numNegativeEntities, numPositiveEntities);        
        
        JavaPairRDD<Integer,IntArrayList> valueResults = topKValueCandidates.mapValues(x -> {
            Map<Integer, Float> rankedCandidates = new HashMap<>();
            //sort by descending value sim
            for (Map.Entry<Integer, Float> entry : x.entrySet()) {
               rankedCandidates.put(entry.getKey(), entry.getValue());
            }                    
            return new IntArrayList(Utils.sortByValue(rankedCandidates, true).keySet());
        });*/
        //end of WJS
        
        
        blocksFromEI.unpersist();    
        
        
        
        //Start the evaluation        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");
        EvaluateBlockingResults evaluation = new EvaluateBlockingResults();
        
        String GT_SEPARATOR = ",";
        
        JavaPairRDD<Integer,Integer> gt = Utils.getGroundTruthIdsFromEntityIds(jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), jsc.textFile(groundTruthPath), GT_SEPARATOR);
        gt.cache();
        gt.saveAsTextFile(groundTruthOutputPath);
        
        System.out.println("Finished loading the ground truth, now evaluating the results...");  
        
        evaluation.evaluateBlockingResults(valueResults, gt, TPs, FPs, FNs);
        System.out.println("Evaluation finished successfully.");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());   
        
        /*
        System.out.println("Getting the top K neighbor candidates...");
        JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates = cnp.run(
                topKValueCandidates, 
                jsc.textFile(inputTriples1, PARALLELISM), 
                jsc.textFile(inputTriples2, PARALLELISM), 
                SEPARATOR, 
                jsc.textFile(entityIds1),
                jsc.textFile(entityIds2),
                MIN_SUPPORT_THRESHOLD, K, N, 
                jsc);
        */
        
        
        spark.stop();
    }
    
    
    
    
}
