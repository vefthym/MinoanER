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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import metablockingspark.entityBased.EntityBasedCNP;
import metablockingspark.preprocessing.BlockFiltering;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.preprocessing.BlocksPerEntity;
import metablockingspark.preprocessing.EntityWeightsWJS;
import metablockingspark.utils.Utils;
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
            master = "spark://master:7077";
            inputPath = args[0];            
            outputPath = args[1];
            // delete existing output directories
            try {                                
                Utils.deleteHDFSPath(outputPath);
            } catch (IOException | URISyntaxException ex) {
                Logger.getLogger(BlockFiltering.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                       
        SparkSession spark = SparkSession.builder()
            .appName("MetaBlocking")
            .config("spark.sql.warehouse.dir", tmpPath)
            .config("spark.eventLog.enabled", true)
            .master(master)
            .getOrCreate();        
        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());        
        LongAccumulator BLOCK_ASSIGNMENTS_ACCUM = jsc.sc().longAccumulator();
        
        
        
        ////////////////////////
        //start the processing//
        ////////////////////////
        
        //Block Filtering
        System.out.println("\n\nStarting BlockFiltering, reading from "+inputPath);
        
        BlockFiltering bf = new BlockFiltering(spark);
        JavaPairRDD<Integer,Integer[]> entityIndex = bf.run(jsc.textFile(inputPath), BLOCK_ASSIGNMENTS_ACCUM); 
        entityIndex.cache();
        //entityIndex.persist(StorageLevel.DISK_ONLY_2()); //store to disk with replication factor 2
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        
        //long numEntities = entityIndex.keys().count();
        
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex");
                
        LongAccumulator CLEAN_BLOCK_ACCUM = jsc.sc().longAccumulator();
        LongAccumulator NUM_COMPARISONS_ACCUM = jsc.sc().longAccumulator();
        
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex(spark, entityIndex);
        JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI = bFromEI.run(CLEAN_BLOCK_ACCUM, NUM_COMPARISONS_ACCUM);
        
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        
        //Blocks Per Entity (not needed for WJS weighting scheme)
        //BlocksPerEntity bpe = new BlocksPerEntity(spark, entityIndex);
        //JavaPairRDD<Integer,Integer> blocksPerEntity = bpe.run();
        //blocksPerEntity.cache();
        //JavaPairRDD<Integer,Integer> blocksPerEntity = entityIndex.mapValues(x-> x.length).cache(); //one-liner to avoid a new class instance
        
        //get the total weights of each entity, required by WJS weigthing scheme (only)
        //Broadcast<JavaPairRDD<Integer, Iterable<Integer>>> blocksFromEI_BV = jsc.broadcast(blocksFromEI);
        EntityWeightsWJS wjsWeights = new EntityWeightsWJS(spark);
        Map<Integer,Double> totalWeights = wjsWeights.getWeights(blocksFromEI, entityIndex); //double[] cannot be used, because some entityIds are negative
        Broadcast<Map<Integer,Double>> totalWeights_BV = jsc.broadcast(totalWeights);
        
        double BCin = (double) BLOCK_ASSIGNMENTS_ACCUM.value() / entityIndex.count(); //BCin = average number of block assignments per entity
        final int K = ((Double)Math.floor(BCin - 1)).intValue(); //K = |_BCin -1_|
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        //CNP
        EntityBasedCNP cnp = new EntityBasedCNP(spark);
        JavaPairRDD<Integer,Integer[]> metablockingResults = cnp.run(blocksFromEI, totalWeights_BV, K);
        
        metablockingResults
                .mapValues(x -> Arrays.toString(x)).saveAsTextFile(outputPath); //only to see the output and add an action (saving to file may not be needed)
//                .collect(); // only to get the execution time (just an action to trigger the execution)
    }
    
}
