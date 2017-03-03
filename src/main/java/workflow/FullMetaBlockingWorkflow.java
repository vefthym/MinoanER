/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package workflow;

import java.io.IOException;
import java.net.URISyntaxException;
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

/**
 *
 * @author vefthym
 */
public class FullMetaBlockingWorkflow {
    
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputPath;
        String blockSizesOutputPath;
        String entityIndexOutputPath;
        String blocksFromEIPath = null; //TODO: add
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:/temp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testInput";
            blockSizesOutputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testOutputBlockSizes";
            entityIndexOutputPath = "/file:C:\\Users\\VASILIS\\Documents\\MetaBlocking\\testOutputEntityIndex";
        } else {            
            tmpPath = "/file:/tmp";
            master = "spark://master:7077";
            inputPath = args[0];
            blockSizesOutputPath = args[1];
            entityIndexOutputPath = args[2];            
            
            // delete existing output directories
            try {                
                Utils.deleteHDFSPath(blockSizesOutputPath);
                Utils.deleteHDFSPath(entityIndexOutputPath);
                Utils.deleteHDFSPath(blocksFromEIPath);
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
        
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        
        //Block Filtering
        System.out.println("\n\nStarting BlockFiltering, reading from "+inputPath+
                " and writing block sizes to "+blockSizesOutputPath+
                " and entity index to "+entityIndexOutputPath);
        
        BlockFiltering bf = new BlockFiltering(spark, blockSizesOutputPath, entityIndexOutputPath);
        JavaPairRDD<Integer,Integer[]> entityIndex = bf.run(sc.textFile(inputPath)); 
        entityIndex.cache();
        //entityIndex.persist(StorageLevel.DISK_ONLY_2()); //store to disk with replication factor 2
        //TODO: check if entityIndex fits in memory (to cache it) or not (to store it in disk)
        
        //long numEntities = entityIndex.keys().count();
        
        //Blocks From Entity Index
        System.out.println("\n\nStarting BlocksFromEntityIndex, reading from "+entityIndexOutputPath+
                " and writing blocks to "+blocksFromEIPath);
                
        BlocksFromEntityIndex bFromEI = new BlocksFromEntityIndex(spark, entityIndex, blocksFromEIPath);
        JavaPairRDD<Integer, Iterable<Integer>> blocksFromEI = bFromEI.run();
        
        //Blocks Per Entity
        //BlocksPerEntity bpe = new BlocksPerEntity(spark, entityIndex);
        //JavaPairRDD<Integer,Integer> blocksPerEntity = bpe.run();
        //blocksPerEntity.cache();
        //JavaPairRDD<Integer,Integer> blocksPerEntity = entityIndex.mapValues(x-> x.length).cache(); //one-liner to avoid a new class instance
        
        //blocks per entity not needed for WJS (perhaps another task is needed to get totalWeights of entities (for each of their tokens)
        EntityWeightsWJS wjsWeights = new EntityWeightsWJS(spark, blocksFromEI, entityIndex);
        Map<Integer,Double> totalWeights = wjsWeights.getWeights(); //double[] cannot be used, because some entityIds are negative
        Broadcast<Map<Integer,Double>> totalWeightsBV = sc.broadcast(totalWeights);
        
        final int K = 0; //TODO: k = BCin
        
        //CNP
        EntityBasedCNP cnp = new EntityBasedCNP(spark,blocksFromEI, totalWeightsBV, K);
        cnp.run();
    }
    
}
