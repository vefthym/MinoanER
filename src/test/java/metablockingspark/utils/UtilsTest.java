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
package metablockingspark.utils;

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author vefthym
 */
public class UtilsTest {
    
    public UtilsTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of sortByValue method, of class Utils.
     */
    @org.junit.Test
    public void testSortByValue() {
        System.out.println("sortByValue");
        Int2FloatOpenHashMap map = new Int2FloatOpenHashMap();
        map.put(3, 0.5f);
        map.put(2, 0.6f);
        map.put(4, 0.4f);
        map.put(1, 0.8f);
        map.put(5, 0f);
        Integer[] arrayResult = new Integer[map.size()];
        arrayResult = Utils.sortByValue(map, true).keySet().toArray(arrayResult);
        Integer[] correctResult = new Integer[]{1,2,3,4,5};
        assertArrayEquals(arrayResult, correctResult);     
                
        arrayResult = Utils.sortByValue(map, false).keySet().toArray(arrayResult);
        correctResult = new Integer[]{5,4,3,2,1};
        assertArrayEquals(arrayResult, correctResult);     
    }


    /**
     * Test of encodeURIinUTF8 method, of class Utils.
     */
    @org.junit.Test
    public void testEncodeURIinUTF8() {
        System.out.println("encodeURIinUTF8");
        String uri = "<http://dbpedia.org/resource/Hello(film)>";
        String expResult = "<http://dbpedia.org/resource/Hello%28film%29>";
        String result = Utils.encodeURIinUTF8(uri);
        assertEquals(expResult, result);
        
        uri = "<http://non-dbpedia.com>";
        expResult = "<http://non-dbpedia.com>";
        result = Utils.encodeURIinUTF8(uri);
        assertEquals(expResult, result);
    }
    
}
