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
package metablockingspark.preprocessing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import metablockingspark.utils.Utils;
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
public class BlockFilteringAdvancedTest {
    
    SparkSession spark;
    JavaSparkContext jsc;
    public BlockFilteringAdvancedTest() {
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
     * Test of run method, of class BlockFilteringAdvanced.
     */
    @Test
    public void testRun() {
        System.out.println("getEntityBlocksAdvanced");
        
        List<String> dummyBlocks = new ArrayList<>();
        dummyBlocks.add("0\t1#2#3#4#5#;-1#-2#-3#-4#-5#");
        dummyBlocks.add("1\t3#4#5#;-1#-5#");
        dummyBlocks.add("2\t5#;-5#");
        dummyBlocks.add("3\t5#;");
        JavaRDD<String> blockingInput = jsc.parallelize(dummyBlocks);
        LongAccumulator BLOCK_ASSIGNMENTS = jsc.sc().longAccumulator();
        
        BlockFilteringAdvanced instance = new BlockFilteringAdvanced();          
        JavaPairRDD<Integer, IntArrayList> result = instance.run(blockingInput, BLOCK_ASSIGNMENTS);
        
        List<Tuple2<Integer,IntArrayList>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>(1, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(2, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(3, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(4, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(5, new IntArrayList(new int[]{2,1})));
        expResult.add(new Tuple2<>(-1, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(-2, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(-3, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(-4, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(-5, new IntArrayList(new int[]{2,1})));
        
        JavaPairRDD<Integer,IntArrayList> expResultRDD = jsc.parallelizePairs(expResult);
        
        List<Tuple2<Integer, IntArrayList>> resultList = result.collect();
        List<Tuple2<Integer, IntArrayList>> expResultList = expResultRDD.collect();
        
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        
        assertEquals(new HashSet<>(resultList), new HashSet<>(expResultList));
        assertEquals((long)BLOCK_ASSIGNMENTS.value(), 15);
    }

    /**
     * Test of parseBlockCollection method, of class BlockFilteringAdvanced.
     */
    @Test
    public void testParseBlockCollection() {
        System.out.println("parseBlockCollection");
        List<String> dummyBlocks = new ArrayList<>();
        dummyBlocks.add("0\t1#2#3#4#5#;-1#-2#-3#-4#-5#");
        dummyBlocks.add("1\t3#4#5#;-1#-5#");
        dummyBlocks.add("2\t5#;-5#");
        dummyBlocks.add("3\t5#;");
        JavaRDD<String> blockingInput = jsc.parallelize(dummyBlocks);
        BlockFilteringAdvanced instance = new BlockFilteringAdvanced();        
        JavaPairRDD<Integer, IntArrayList> result = instance.parseBlockCollection(blockingInput);
        
        List<Tuple2<Integer,IntArrayList>> dummyBlocksParsed = new ArrayList<>();
        dummyBlocksParsed.add(new Tuple2<>(0, new IntArrayList(new int[]{1,2,3,4,5,-1,-2,-3,-4,-5})));
        dummyBlocksParsed.add(new Tuple2<>(1, new IntArrayList(new int[]{3,4,5,-1,-5})));
        dummyBlocksParsed.add(new Tuple2<>(2, new IntArrayList(new int[]{5,-5})));
        dummyBlocksParsed.add(new Tuple2<>(3, new IntArrayList(new int[]{5})));
        JavaPairRDD<Integer, IntArrayList> expResult = jsc.parallelizePairs(dummyBlocksParsed);
        
        List<Tuple2<Integer, IntArrayList>> resultList = result.collect();
        List<Tuple2<Integer, IntArrayList>> expResultList = expResult.collect();
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        assertEquals(resultList, expResultList);
    }
    
    /**
     * Test of getEntityBlocksAdvanced method, of class BlockFilteringAdvanced.
     * @throws java.lang.IllegalAccessException
     * @throws java.lang.reflect.InvocationTargetException
     * @throws java.lang.NoSuchMethodException
     */
    @Test
    public void testGetEntityBlocksAdvanced() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException {
        System.out.println("getEntityBlocksAdvanced");
        
        List<String> dummyBlocks = new ArrayList<>();
        dummyBlocks.add("0\t1#2#3#4#5#;-1#-2#-3#-4#-5#");
        dummyBlocks.add("1\t3#4#5#;-1#-5#");
        dummyBlocks.add("2\t5#;-5#");
        dummyBlocks.add("3\t5#;");
        JavaRDD<String> blockingInput = jsc.parallelize(dummyBlocks);
        BlockFilteringAdvanced instance = new BlockFilteringAdvanced();        
        JavaPairRDD<Integer, IntArrayList> parsedBlocks = instance.parseBlockCollection(blockingInput);
        
        Method method = BlockFilteringAdvanced.class.getDeclaredMethod("getEntityBlocksAdvanced", JavaPairRDD.class);
        method.setAccessible(true);
        
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> result = (JavaPairRDD<Integer, Tuple2<Integer, Integer>>) method.invoke(instance, parsedBlocks);        
        
        List<Tuple2<Integer,Tuple2<Integer,Integer>>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>(1, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(2, new Tuple2<>(0,5)));        
        expResult.add(new Tuple2<>(3, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(4, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(5, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(-1, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(-2, new Tuple2<>(0,5)));        
        expResult.add(new Tuple2<>(-3, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(-4, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(-5, new Tuple2<>(0,5)));
        expResult.add(new Tuple2<>(3, new Tuple2<>(1,3)));
        expResult.add(new Tuple2<>(4, new Tuple2<>(1,3)));
        expResult.add(new Tuple2<>(5, new Tuple2<>(1,3)));
        expResult.add(new Tuple2<>(-1, new Tuple2<>(1,3)));
        expResult.add(new Tuple2<>(-5, new Tuple2<>(1,3)));
        expResult.add(new Tuple2<>(5, new Tuple2<>(2,1)));
        expResult.add(new Tuple2<>(-5, new Tuple2<>(2,1)));
        //expResult.add(new Tuple2<>(5, new Tuple2<>(3,0))); //null result
        
        
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> expResultRDD = jsc.parallelizePairs(expResult);
               
        List<Tuple2<Integer, Tuple2<Integer, Integer>>> resultList = result.collect();
        List<Tuple2<Integer, Tuple2<Integer, Integer>>> expResultList = expResultRDD.collect();
        
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        
        assertEquals(new HashSet<>(resultList), new HashSet<>(expResultList));
    }
    
    /**
     * Test of getEntityIndex method, of class BlockFilteringAdvanced.
     */    
    @Test
    public void testGetEntityIndex() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        System.out.println("getEntityIndex");
        
        List<String> dummyBlocks = new ArrayList<>();
        dummyBlocks.add("0\t1#2#3#4#5#;-1#-2#-3#-4#-5#");
        dummyBlocks.add("1\t3#4#5#;-1#-5#");
        dummyBlocks.add("2\t5#;-5#");
        dummyBlocks.add("3\t5#;");
        JavaRDD<String> blockingInput = jsc.parallelize(dummyBlocks);
        BlockFilteringAdvanced instance = new BlockFilteringAdvanced();        
        JavaPairRDD<Integer, IntArrayList> parsedBlocks = instance.parseBlockCollection(blockingInput);
        Method method1 = BlockFilteringAdvanced.class.getDeclaredMethod("getEntityBlocksAdvanced", JavaPairRDD.class);
        method1.setAccessible(true);        
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> entityBlocks = (JavaPairRDD<Integer, Tuple2<Integer, Integer>>) method1.invoke(instance, parsedBlocks);
        
        Method method2 = BlockFilteringAdvanced.class.getDeclaredMethod("getEntityIndex", JavaPairRDD.class, LongAccumulator.class);
        method2.setAccessible(true);        
        LongAccumulator BLOCK_ASSIGNMENTS = jsc.sc().longAccumulator();
        JavaPairRDD<Integer, IntArrayList> result = (JavaPairRDD<Integer, IntArrayList>) method2.invoke(instance, entityBlocks, BLOCK_ASSIGNMENTS);        
        
        //final int MAX_BLOCKS = ((Double)Math.floor(3*numBlocks/4+1)).intValue(); //|_ 3|Bi|/4+1 _| //preprocessing
        
        List<Tuple2<Integer,IntArrayList>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>(-2, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(4, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(-1, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(-5, new IntArrayList(new int[]{2,1})));
        expResult.add(new Tuple2<>(-4, new IntArrayList(new int[]{0})));        
        expResult.add(new Tuple2<>(1, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(-3, new IntArrayList(new int[]{0})));
        expResult.add(new Tuple2<>(3, new IntArrayList(new int[]{1,0})));
        expResult.add(new Tuple2<>(5, new IntArrayList(new int[]{2,1})));
        expResult.add(new Tuple2<>(2, new IntArrayList(new int[]{0})));
               
        JavaPairRDD<Integer,IntArrayList> expResultRDD = jsc.parallelizePairs(expResult);
        
        List<Tuple2<Integer, IntArrayList>> resultList = result.collect();
        List<Tuple2<Integer, IntArrayList>> expResultList = expResultRDD.collect();
        
        System.out.println("Result: "+Arrays.toString(resultList.toArray()));
        System.out.println("Expect: "+Arrays.toString(expResultList.toArray()));
        
        assertEquals(new HashSet<>(resultList), new HashSet<>(expResultList));        
        assertEquals((long)BLOCK_ASSIGNMENTS.value(), 15);
    }
}
