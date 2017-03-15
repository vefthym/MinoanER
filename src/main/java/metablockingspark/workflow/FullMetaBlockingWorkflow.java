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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.entityBased.EntityBasedCNP;
import metablockingspark.entityBased.EntityBasedCNPInMemory;
import metablockingspark.preprocessing.BlockFiltering;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.preprocessing.EntityWeightsWJS;
import metablockingspark.utils.MyKryoRegistrator;
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
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";            
            outputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else {            
            tmpPath = "/file:/tmp";
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
                       
        final int NUM_CORES_IN_CLUSTER = 128; //128 in ISL cluster, 28 in okeanos cluster
                       
        SparkSession spark = SparkSession.builder()
            .appName("MetaBlocking WJS on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1)) 
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .config("spark.default.parallelism", NUM_CORES_IN_CLUSTER * 4) //x tasks for each core (128 cores) --> x "reduce" rounds
            .config("spark.rdd.compress", true)
            
            //memory configurations (deprecated)            
            .config("spark.memory.useLegacyMode", true)
            .config("spark.shuffle.memoryFraction", 0.4)
            .config("spark.storage.memoryFraction", 0.4)                
            .config("spark.memory.offHeap.enabled", true)
            .config("spark.memory.offHeap.size", "10g")            
            
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
        JavaPairRDD<Integer,Integer[]> entityIndex = bf.run(jsc.textFile(inputPath), BLOCK_ASSIGNMENTS_ACCUM); 
        entityIndex.cache();
        //entityIndex.persist(StorageLevel.DISK_ONLY_2()); //store to disk with replication factor 2
        
        
        //long numEntities = entityIndex.keys().count();
        
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex...");
                
        LongAccumulator CLEAN_BLOCK_ACCUM = jsc.sc().longAccumulator();
        LongAccumulator NUM_COMPARISONS_ACCUM = jsc.sc().longAccumulator();
        
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex();
        JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI = bFromEI.run(entityIndex, CLEAN_BLOCK_ACCUM, NUM_COMPARISONS_ACCUM);
        blocksFromEI.persist(StorageLevel.DISK_ONLY());
        
        
        //get the total weights of each entity, required by WJS weigthing scheme (only)
        //Broadcast<JavaPairRDD<Integer, Iterable<Integer>>> blocksFromEI_BV = jsc.broadcast(blocksFromEI);
        System.out.println("\n\nStarting EntityWeightsWJS...");
        EntityWeightsWJS wjsWeights = new EntityWeightsWJS();
        Broadcast<JavaPairRDD<Integer,Float>> totalWeights_BV = jsc.broadcast(wjsWeights.getWeights(blocksFromEI, entityIndex)); //double[] cannot be used, because some entityIds are negative
        //System.out.println("Total weights contain weights for "+totalWeights.size()+" entities.");
        //Broadcast<Map<Integer,Double>> totalWeights_BV = jsc.broadcast(totalWeights);
        
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
        //EntityBasedCNPInMemory cnp = new EntityBasedCNPInMemory();
        EntityBasedCNP cnp = new EntityBasedCNP();
        JavaPairRDD<Integer,IntArrayList> metablockingResults = cnp.run(blocksFromEI, totalWeights_BV, K, numNegativeEntities, numPositiveEntities);
        
        metablockingResults
                .mapValues(x -> Arrays.toString(x.elements())).saveAsTextFile(outputPath); //only to see the output and add an action (saving to file may not be needed)
        System.out.println("Job finished successfully. Output written in "+outputPath);
    }
    
}
