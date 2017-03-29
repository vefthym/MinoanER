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

package metablockingspark.workflow;

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablocking.matching.ReciprocalMatching;
import metablockingspark.entityBased.neighbors.EntityBasedCNPNeighborsInMemory;
import metablockingspark.evaluation.EvaluateMatchingResults;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.preprocessing.EntityWeightsWJS;
import metablockingspark.rankAggregation.LocalRankAggregation;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author vefthym
 */
public class FullMetaBlockingWorkflow {
    
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputPath;      
        String outputPath;
        String inputTriples1, inputTriples2;
        
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";  
            inputTriples1 = "";
            inputTriples2 = "";
            outputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else if (args.length == 4) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            inputPath = args[0];
            inputTriples1 = args[1];
            inputTriples2 = args[2];
            outputPath = args[3];
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(outputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(FullMetaBlockingWorkflow.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            System.out.println("You can run Metablocking with the following arguments:"
                    + "0: inputBlocking"
                    + "1: inputTriples1 (raw rdf triples)"
                    + "2: inputTriples2 (raw rdf triples)"
                    + "3: outputPath");
            return;
        }
                       
        //tuning params
        final int NUM_CORES_IN_CLUSTER = 152; //152 in ISL cluster, 28 in okeanos cluster
        final int NUM_WORKERS = 4; //4 in ISL cluster, 14 in okeanos cluster
        final int NUM_EXECUTORS = NUM_WORKERS * 3;
        final int NUM_EXECUTOR_CORES = NUM_CORES_IN_CLUSTER/NUM_EXECUTORS;
        final int PARALLELISM = NUM_EXECUTORS * NUM_EXECUTOR_CORES * 3; //spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
                       
        SparkSession spark = SparkSession.builder()
            .appName("FullMetaBlocking WJS on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1)) 
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", PARALLELISM) //x tasks for each core --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "20s")    
                
            .config("spark.executor.instances", NUM_EXECUTORS)
            .config("spark.executor.cores", NUM_EXECUTOR_CORES)
            .config("spark.executor.memory", "50G")
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
            
            //un-comment the following for Kryo serializer (does not seem to improve compression, only speed)            
            /*
            .config("spark.kryo.registrator", MyKryoRegistrator.class.getName())
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrationRequired", true) //just to be safe that everything is serialized as it should be (otherwise runtime error)
            */
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
        System.out.println("\n\nStarting EntityWeightsWJS...");
        EntityWeightsWJS wjsWeights = new EntityWeightsWJS();        
        Int2FloatOpenHashMap totalWeights = new Int2FloatOpenHashMap(); //saves memory by storing data as primitive types        
        wjsWeights.getWeights(blocksFromEI, entityIndex).foreach(entry -> {
            totalWeights.put(entry._1().intValue(), entry._2().floatValue());
        });
        Broadcast<Int2FloatOpenHashMap> totalWeights_BV = jsc.broadcast(totalWeights);        
        
        double BCin = (double) BLOCK_ASSIGNMENTS_ACCUM.value() / entityIndex.count(); //BCin = average number of block assignments per entity
        final int K = ((Double)Math.floor(BCin - 1)).intValue(); //K = |_BCin -1_|
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        long numNegativeEntities = wjsWeights.getNumNegativeEntities();
        long numPositiveEntities = wjsWeights.getNumPositiveEntities();
        
        System.out.println("Found "+numNegativeEntities+" negative entities");
        System.out.println("Found "+numPositiveEntities+" positive entities");
        
        //CNP
        System.out.println("\n\nStarting CNP...");
        String SEPARATOR = " ";
        if (inputTriples1.endsWith(".tsv")) {
            SEPARATOR = "\t";
        }
        final float MIN_SUPPORT_THRESHOLD = 0.01f;
        final int N = 3; //for top-N neighbors
        
        System.out.println("Getting the top K value candidates...");
        EntityBasedCNPNeighborsInMemory cnp = new EntityBasedCNPNeighborsInMemory();        
        JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates = cnp.getTopKValueSims(blocksFromEI, totalWeights_BV, K, numNegativeEntities, numPositiveEntities);
        //totalWeights_BV.unpersist();
        //totalWeights_BV.destroy();
        
        blocksFromEI.unpersist();
        //topKValueCandidates.setName("topKValueCandidates").persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        System.out.println("Getting the top K neighbor candidates...");
        JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates = cnp.run(topKValueCandidates, jsc.textFile(inputTriples1), jsc.textFile(inputTriples2), SEPARATOR, MIN_SUPPORT_THRESHOLD, K, N, jsc, PARALLELISM);
        
        //rank aggregation        
        System.out.println("Starting rank aggregation...");
        JavaPairRDD<Integer,Integer> aggregates = new LocalRankAggregation().getTopCandidatePerEntity(topKValueCandidates, topKNeighborCandidates);
        
        //reciprocal matching
        System.out.println("Starting reciprocal matching...");
        JavaPairRDD<Integer,Integer> matches = new ReciprocalMatching().getReciprocalMatches(aggregates);
        
        //comment-out the following when using evaluation
        System.out.println("Writing results to HDFS...");
        matches.saveAsTextFile(outputPath); //only to see the output and add an action (saving to file may not be needed)        
        System.out.println("Job finished successfully. Output written in "+outputPath);
        
        /*
        long numMatches = matches.count(); //only to add an action
        System.out.println("Job finished successfully. Found "+numMatches+" matches. Now starting the evaluation...");
        
        
        
        //unpersist all RDDs and destroy all Broadcasts (not sure if needed)
        totalWeights_BV.unpersist();
        totalWeights_BV.destroy();
        
        //Start the evaluation        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");
        EvaluateMatchingResults evaluation = new EvaluateMatchingResults();
        
        JavaPairRDD<Integer,Integer> gt = Utils.getGroundTruthIds(jsc.textFile(inputTriples1), jsc.textFile(inputTriples2), SEPARATOR, jsc.textFile("gtPath"), SEPARATOR);
        evaluation.evaluateResults(matches, gt, TPs, FPs, FNs);
        System.out.println("Evaluation finished successfully.");
        evaluation.printResults(TPs.value(), FPs.value(), FNs.value());                
        */
        
        spark.stop();
    }
    
}
