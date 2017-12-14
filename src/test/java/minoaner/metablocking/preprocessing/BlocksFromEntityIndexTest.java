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
package minoaner.metablocking.preprocessing;

import minoaner.metablocking.preprocessing.BlocksFromEntityIndex;
import minoaner.metablocking.preprocessing.BlockFilteringAdvanced;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
public class BlocksFromEntityIndexTest {
    
    SparkSession spark;
    JavaSparkContext jsc;
    public BlocksFromEntityIndexTest() {
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
     * Test of run method, of class BlocksFromEntityIndex.
     */
    @Test
    public void testRun() {
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
        
        BlocksFromEntityIndex instance = new BlocksFromEntityIndex();
        LongAccumulator cleanBlocksAccum = jsc.sc().longAccumulator();
        LongAccumulator numComparisons = jsc.sc().longAccumulator();
        JavaPairRDD<Integer, IntArrayList> result = instance.run(entityIndex, cleanBlocksAccum, numComparisons);
        
        List<Tuple2<Integer,IntArrayList>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>(0, new IntArrayList(new int[]{1,2,3,4,-1,-2,-3,-4})));
        expResult.add(new Tuple2<>(1, new IntArrayList(new int[]{3,4,5,-1,-5})));
        expResult.add(new Tuple2<>(2, new IntArrayList(new int[]{5,-5})));      
        
        JavaPairRDD<Integer,IntArrayList> expResultRDD = jsc.parallelizePairs(expResult);
        
        List<Tuple2<Integer, IntArrayList>> resultList = result.collect();
        List<Tuple2<Integer, IntArrayList>> expResultList = expResultRDD.collect();
        
        expResultList.stream().forEach(listItem -> Collections.sort(listItem._2()));
        resultList.stream().forEach(listItem -> Collections.sort(listItem._2()));
        
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        
        assertEquals((long)cleanBlocksAccum.value(), 3);
        assertEquals((long)numComparisons.value(), 23);
        assertEquals(new HashSet<>(resultList), new HashSet<>(expResultList));
        
        //assertEquals(expResultRDD, result);
    }
    
}
