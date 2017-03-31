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

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author vefthym
 */
public abstract class BlockingEvaluation {
    
    
    /**
     * Compute precision, recall, f-measure of the input results, given the ground truth. 
     * The input RDDs should be in the same format (negative entity Id, positive entity Id).
     * @param blockingResults the blocking results in the form (-entityId, +entityId)
     * @param groundTruth the ground truth in the form (-entityId, +entityId)
     * @param TPs true positives to update (true matches)
     * @param FPs false positives to update (false matches)
     * @param FNs false negatives to update (missed matches)
     */
    public void evaluateBlockingResults(JavaPairRDD<Integer,IntArrayList> blockingResults, JavaPairRDD<Integer,Integer> groundTruth, LongAccumulator TPs, LongAccumulator FPs, LongAccumulator FNs) {
        blockingResults
                .fullOuterJoin(groundTruth)
                .foreach(joinedMatch -> {
                    IntArrayList myCandidates = joinedMatch._2()._1().orElse(null);
                    Integer correctResult = joinedMatch._2()._2().orElse(null);
                    if (myCandidates == null) { //this means that the correct result is not null (otherwise, nothing to join here)
                        FNs.add(1); //missed match
                    } else if (correctResult == null) {
                        FPs.add(myCandidates.size()); //each candidate is a false match (no candidate should exist)
                    } else if (myCandidates.contains(correctResult)) {
                        TPs.add(1); //true match
                        FPs.add(myCandidates.size()-1); //the rest are false matches (ideal: only one candidate suggested)
                    } else {        //then the correct result is not included in my candidates => I missed this match and all my candidates are wrong
                        FPs.add(myCandidates.size()); //all my candidates were wrong 
                        FNs.add(1); //the correct match was missed
                    }                    
                });
    }
    
}
