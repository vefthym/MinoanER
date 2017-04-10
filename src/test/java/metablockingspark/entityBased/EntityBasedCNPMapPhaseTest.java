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
package metablockingspark.entityBased;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import metablockingspark.preprocessing.BlockFilteringAdvanced;
import metablockingspark.preprocessing.BlocksFromEntityIndex;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EntityBasedCNPMapPhaseTest {
    
    SparkSession spark;
    JavaSparkContext jsc;
    public EntityBasedCNPMapPhaseTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        System.setProperty("hadoop.home.dir", "C:\\Users\\VASILIS\\Documents\\hadoop_home"); //only for local mode
        
        spark = SparkSession.builder()
            .appName("test") 
            .config("spark.sql.warehouse.dir", "/file:/tmp")                
            .config("spark.executor.instances", 1)
            .config("spark.executor.cores", 1)
            .config("spark.executor.memory", "1G")            
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.master", "local")
            .getOrCreate();        
        
        
        
        jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); 
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getMapOutput method, of class EntityBasedCNPMapPhase.
     */
    /*
    @Test
    public void testGetMapOutput() {
        System.out.println("getMapOutput");
        JavaPairRDD<Integer, IntArrayList> blocksFromEI = null;
        JavaPairRDD<Integer, IntArrayList> expResult = null;
        JavaPairRDD<Integer, IntArrayList> result = EntityBasedCNPMapPhase.getMapOutput(blocksFromEI);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }*/

    /**
     * Test of getMapOutputWJS method, of class EntityBasedCNPMapPhase.
     */
    @Test
    public void testGetMapOutputWJS() {
        System.out.println("getMapOutputWJS");
        
        System.out.println("blocks from entity index");
        List<String> dummyBlocks = new ArrayList<>();
        dummyBlocks.add("0\t1#2#3#4#5#;-1#-2#-3#-4#-5#");
        dummyBlocks.add("1\t3#4#5#;-1#-5#");
        dummyBlocks.add("2\t5#;-5#");
        dummyBlocks.add("3\t5#;");
        JavaRDD<String> blockingInput = jsc.parallelize(dummyBlocks);
        LongAccumulator BLOCK_ASSIGNMENTS = jsc.sc().longAccumulator();
        
        BlockFilteringAdvanced blockFiltering = new BlockFilteringAdvanced();          
        JavaPairRDD<Integer, IntArrayList> entityIndex = blockFiltering.run(blockingInput, BLOCK_ASSIGNMENTS);
        
        BlocksFromEntityIndex bfei = new BlocksFromEntityIndex();
        LongAccumulator cleanBlocksAccum = jsc.sc().longAccumulator();
        LongAccumulator numComparisons = jsc.sc().longAccumulator();
        JavaPairRDD<Integer, IntArrayList> filteredBlocks = bfei.run(entityIndex, cleanBlocksAccum, numComparisons);
        
        List<Tuple2<Integer,IntArrayList>> tweakedBlocks = new ArrayList<>(filteredBlocks.collect());        
        tweakedBlocks.add(new Tuple2<>(-1, new IntArrayList(new int[]{-100}))); //this should not alter the results
        filteredBlocks = jsc.parallelizePairs(tweakedBlocks); 
        
        JavaPairRDD<Integer, IntArrayList> result = EntityBasedCNPMapPhase.getMapOutputWJS(filteredBlocks);
        
        List<Tuple2<Integer,IntArrayList>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>(4, new IntArrayList(new int[]{5, -2, -1, -5, -4, -3})));
        expResult.add(new Tuple2<>(1, new IntArrayList(new int[]{5, -2, -1, -5, -4, -3})));
        expResult.add(new Tuple2<>(3, new IntArrayList(new int[]{5, -2, -1, -5, -4, -3})));
        expResult.add(new Tuple2<>(5, new IntArrayList(new int[]{5, -2, -1, -5, -4, -3})));
        expResult.add(new Tuple2<>(2, new IntArrayList(new int[]{5, -2, -1, -5, -4, -3})));
        expResult.add(new Tuple2<>(-2, new IntArrayList(new int[]{5, 4, 1, 3, 5, 2})));
        expResult.add(new Tuple2<>(-1, new IntArrayList(new int[]{5, 4, 1, 3, 5, 2})));
        expResult.add(new Tuple2<>(-5, new IntArrayList(new int[]{5, 4, 1, 3, 5, 2})));
        expResult.add(new Tuple2<>(-4, new IntArrayList(new int[]{5, 4, 1, 3, 5, 2})));
        expResult.add(new Tuple2<>(-3, new IntArrayList(new int[]{5, 4, 1, 3, 5, 2})));
        expResult.add(new Tuple2<>(4, new IntArrayList(new int[]{3, -1, -5})));
        expResult.add(new Tuple2<>(3, new IntArrayList(new int[]{3, -1, -5})));
        expResult.add(new Tuple2<>(5, new IntArrayList(new int[]{3, -1, -5})));
        expResult.add(new Tuple2<>(-1, new IntArrayList(new int[]{2, 4, 3, 5})));
        expResult.add(new Tuple2<>(-5, new IntArrayList(new int[]{2, 4, 3, 5})));
        expResult.add(new Tuple2<>(5, new IntArrayList(new int[]{1, -5})));
        expResult.add(new Tuple2<>(-5, new IntArrayList(new int[]{1, 5})));
        
        JavaPairRDD<Integer, IntArrayList> expResultRDD = jsc.parallelizePairs(expResult);
        
        List<Tuple2<Integer, IntArrayList>> resultList = result.collect();
        List<Tuple2<Integer, IntArrayList>> expResultList = expResultRDD.collect();
        
        //expResultList.stream().forEach(listItem -> Collections.sort(listItem._2()));
        //resultList.stream().forEach(listItem -> Collections.sort(listItem._2()));
        
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        
        assertEquals(new HashSet<>(resultList), new HashSet<>(expResultList));
        
    }
    
}
