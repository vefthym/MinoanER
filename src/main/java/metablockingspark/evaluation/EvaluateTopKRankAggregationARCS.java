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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.List;
import metablockingspark.entityBased.neighbors.EntityBasedCNPNeighborsARCS;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import metablockingspark.rankAggregation.LocalRankAggregation;
import metablockingspark.utils.Utils;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EvaluateTopKRankAggregationARCS {
    
    
    
    public static void main(String[] args) {
        String tmpPath;
        String master;
        String inputPath;      
        String groundTruthPath;
        String inputTriples1, inputTriples2;
        String entityIds1, entityIds2;
        
        
        if (args.length == 0) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
            
            tmpPath = "/file:C:\\tmp";
            master = "local[2]";
            inputPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testInput";  
            inputTriples1 = "";
            inputTriples2 = "";
            entityIds1 = "";
            entityIds2 = "";
            groundTruthPath = "/file:C:\\Users\\VASILIS\\Documents\\OAEI_Datasets\\exportedBlocks\\testOutput";            
        } else if (args.length >= 6) {            
            tmpPath = "/file:/tmp";
            //master = "spark://master:7077";
            inputPath = args[0];
            inputTriples1 = args[1];
            inputTriples2 = args[2];
            entityIds1 = args[3];
            entityIds2 = args[4];
            groundTruthPath = args[5];
        } else {
            System.out.println("You can run Metablocking with the following arguments:"
                    + "0: inputBlocking"
                    + "1: inputTriples1 (raw rdf triples)"
                    + "2: inputTriples2 (raw rdf triples)"
                    + "3: entityIds1: entityUrl\tentityId (positive)"
                    + "4: entityIds2: entityUrl\tentityId (also positive)"
                    + "5: ground truth path"
                    + "6: L (num of aggregation results to keep)");
            return;
        }
        
        String appName = "ARCS topK rank aggregation on "+inputPath.substring(inputPath.lastIndexOf("/", inputPath.length()-2)+1);
        SparkSession spark = Utils.setUpSpark(appName, 3, tmpPath);
        int PARALLELISM = spark.sparkContext().getConf().getInt("spark.default.parallelism", 152);        
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); 
                       
        
        ////////////////////////
        //start the processing//
        ////////////////////////
        
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
        final int K = ((Double)Math.floor(BCin - 1)).intValue(); //K = |_BCin -1_|
        System.out.println(BLOCK_ASSIGNMENTS_ACCUM.value()+" block assignments");
        System.out.println(CLEAN_BLOCK_ACCUM.value()+" clean blocks");
        System.out.println(NUM_COMPARISONS_ACCUM.value()+" comparisons");
        System.out.println("BCin = "+BCin);
        System.out.println("K = "+K);
        
        entityIndex.unpersist();
        
        //CNP
        System.out.println("\n\nStarting CNP...");
        String SEPARATOR = (inputTriples1.endsWith(".tsv"))? "\t" : " ";        
        final float MIN_SUPPORT_THRESHOLD = 0.01f;
        final int N = 3; //for top-N neighbors
        
        System.out.println("Getting the top K value candidates...");
        EntityBasedCNPNeighborsARCS cnp = new EntityBasedCNPNeighborsARCS();        
        JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates = cnp.getTopKValueSims(blocksFromEI, K);
        
        blocksFromEI.unpersist();        
        
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
        
        final int L = (args.length == 7) ? Integer.parseInt(args[6]) : 5; //the default value is 5
        
        //rank aggregation        
        System.out.println("Starting rank aggregation, keeping top "+L+" aggregate candidates per entity...");
        LongAccumulator LISTS_WITH_COMMON_CANDIDATES = jsc.sc().longAccumulator();
        JavaPairRDD<Integer,IntArrayList> aggregationResults = 
                new LocalRankAggregation().getTopKCandidatesPerEntity(
                        topKValueCandidates, 
                        topKNeighborCandidates, 
                        LISTS_WITH_COMMON_CANDIDATES, 
                        L);
        
        aggregationResults.cache();
        long numResults = aggregationResults.count();
        System.out.println(LISTS_WITH_COMMON_CANDIDATES.value()+"/"+numResults+" lists have at least one candidate in common");
        
                
        System.out.println("Starting the evaluation...");             
        
        String GT_SEPARATOR = ",";
        if (groundTruthPath.contains("music")) {
            GT_SEPARATOR = " ";
        }
        
        //load the results        
        JavaPairRDD<Integer,IntArrayList> negativeIdResults = aggregationResults
                .filter(x-> x._1() < 0);
        
        JavaPairRDD<Integer, IntArrayList> positiveIdResults = aggregationResults
                .filter(x-> x._1() >= 0)                
                .flatMapToPair(x -> {
                    List<Tuple2<Integer,Integer>> candidates = new ArrayList<>();
                    for (int candidate : x._2()) {
                        candidates.add(new Tuple2<>(candidate, x._1())); //reverse the pairs
                    }
                    return candidates.iterator();
                })
                .aggregateByKey(new IntOpenHashSet(), 
                        (x,y) -> {x.add(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .mapValues(x-> new IntArrayList(x));
        
        negativeIdResults.cache();
        positiveIdResults.cache();
        
        JavaPairRDD<Integer,IntArrayList> aggregationResultsFinal = 
                negativeIdResults.fullOuterJoin(positiveIdResults)
                .mapValues(x-> {
                    IntArrayList list1 = x._1().orElse(new IntArrayList());
                    IntArrayList list2 = x._2().orElse(new IntArrayList());
                    IntOpenHashSet resultSet = new IntOpenHashSet(list1);
                    resultSet.addAll(list2);
                    return new IntArrayList(resultSet);
                });
        
        //Start the evaluation        
        JavaPairRDD<Integer,Integer> gt = Utils.getGroundTruthIdsFromEntityIds(jsc.textFile(entityIds1, PARALLELISM), jsc.textFile(entityIds2, PARALLELISM), jsc.textFile(groundTruthPath), GT_SEPARATOR);
        gt.cache();        
        
        System.out.println("Finished loading the ground truth with "+ gt.count()+" matches, now evaluating the results...");
        
        LongAccumulator TPs = jsc.sc().longAccumulator("TPs");
        LongAccumulator FPs = jsc.sc().longAccumulator("FPs");
        LongAccumulator FNs = jsc.sc().longAccumulator("FNs");        
        EvaluateBlockingResults blockingEvaluation = new EvaluateBlockingResults();
        
        blockingEvaluation.evaluateBlockingResults(aggregationResultsFinal, gt, TPs, FPs, FNs, false);        
        System.out.println("Found "+aggregationResultsFinal.count()+" entities to be matched");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());           
        
        TPs.reset();
        FPs.reset();
        FNs.reset();
        
        blockingEvaluation.evaluateBlockingResults(negativeIdResults, gt, TPs, FPs, FNs, false);        
        System.out.println("\nFound "+negativeIdResults.count()+" entities to be matched from negative entities");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());           
        
        TPs.reset();
        FPs.reset();
        FNs.reset();
        
        blockingEvaluation.evaluateBlockingResults(positiveIdResults, gt, TPs, FPs, FNs, false);        
        System.out.println("\nFound "+positiveIdResults.count()+" entities to be matched from positive entities");
        EvaluateMatchingResults.printResults(TPs.value(), FPs.value(), FNs.value());           
        
        spark.stop();
    }
    
}
