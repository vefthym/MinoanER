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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class EvaluateMatchingResults {
    
    /**
     * Compute precision, recall, f-measure of the input results, given the ground truth. 
     * The input RDDs should be in the same format (negative entity Id, positive entity Id).
     * @param results the matching results in the form (-entityId, +entityId)
     * @param groundTruth the ground truth in the form (-entityId, +entityId)
     * @param TPs
     * @param FPs
     * @param FNs
     */
    public void evaluateResults(JavaPairRDD<Integer,Integer> results, JavaPairRDD<Integer,Integer> groundTruth, LongAccumulator TPs, LongAccumulator FPs, LongAccumulator FNs) {
        results
                .fullOuterJoin(groundTruth)
                .foreach(joinedMatch -> {
                    Integer myResult = joinedMatch._2()._1().orElse(null);
                    Integer correctResult = joinedMatch._2()._2().orElse(null);
                    if (myResult == null) {
                        FNs.add(1); //missed match
                    } else if (correctResult == null) {
                        FPs.add(1); //wrong match
                    } else if (myResult.equals(correctResult)) {
                        TPs.add(1); //true match
                    } else {        //then I gave a different result than the correct match
                        FPs.add(1); //my result was wrong 
                        FNs.add(1); //the correct match was missed
                    }                    
                });
    }
    
    
    public void printResults(long tps, long fps, long fns) {
        double precision = (double) tps / (tps + fps);
        double recall = (double) tps / (tps + fns);
        double fMeasure = 2 * (precision * recall) / (precision + recall);
        
        System.out.println("Precision = "+precision+" ("+tps+"/"+(tps+fps)+")");
        System.out.println("Recall = "+recall+" ("+tps+"/"+(tps+fns)+")");
        System.out.println("F-measure = "+ fMeasure);
    }
}
