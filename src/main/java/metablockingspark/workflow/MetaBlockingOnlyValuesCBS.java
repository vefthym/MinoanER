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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.entityBased.EntityBasedCNPCBS;
import metablockingspark.preprocessing.BlockFiltering;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author vefthym
 */
public class MetaBlockingOnlyValuesCBS {
    
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputPath;        
        String outputPath;
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";            
            outputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else {            
            tmpPath = "/file:/tmp/";
            //master = "spark://master:7077";
            inputPath = args[0];            
            outputPath = args[1];
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(outputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(BlockFiltering.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        //tuning params
        final int NUM_CORES_IN_CLUSTER = 152; //152 in ISL cluster, 28 in okeanos cluster
        final int NUM_WORKERS = 4; //4 in ISL cluster, 14 in okeanos cluster
        final int NUM_EXECUTORS = NUM_WORKERS * 3;
        final int NUM_EXECUTOR_CORES = NUM_CORES_IN_CLUSTER/NUM_EXECUTORS;
        final int PARALLELISM = NUM_EXECUTORS * NUM_EXECUTOR_CORES * 3; //spark tuning documentation suggests 2 or 3, unless OOM error (in that case more)
                       
        SparkSession spark = SparkSession.builder()
            .appName("MetaBlocking CBS only values on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1)) 
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
        entityIndex.cache();        
                
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex...");
                
        LongAccumulator CLEAN_BLOCK_ACCUM = jsc.sc().longAccumulator();
        LongAccumulator NUM_COMPARISONS_ACCUM = jsc.sc().longAccumulator();
        
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex();
        JavaPairRDD<Integer, IntArrayList> blocksFromEI = bFromEI.run(entityIndex, CLEAN_BLOCK_ACCUM, NUM_COMPARISONS_ACCUM);
        blocksFromEI.persist(StorageLevel.DISK_ONLY());
        
        blocksFromEI.count(); //the simplest action just to run blocksFromEI and get the actual value for the counters below
        
        double BCin = (double) BLOCK_ASSIGNMENTS_ACCUM.value() / entityIndex.count(); //BCin = average number of block assignments per entity
        final int K = ((Double)Math.floor(BCin - 1)).intValue(); //K = |_BCin -1_|
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        //CNP
        System.out.println("\n\nStarting CNP...");
        EntityBasedCNPCBS cnp = new EntityBasedCNPCBS();
        JavaPairRDD<Integer,IntArrayList> metablockingResults = cnp.run(blocksFromEI, K);
        
        metablockingResults
                .mapValues(x -> x.toString()).saveAsTextFile(outputPath); //only to see the output and add an action (saving to file may not be needed)
        System.out.println("Job finished successfully. Output written in "+outputPath);
    }
    
}
