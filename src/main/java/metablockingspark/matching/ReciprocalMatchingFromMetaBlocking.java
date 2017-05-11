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
package metablockingspark.matching;

import it.unimi.dsi.fastutil.ints.Int2FloatLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class ReciprocalMatchingFromMetaBlocking {
    
    public JavaPairRDD<Integer,Integer> getReciprocalMatchesFromTop1Candidates(JavaPairRDD<Integer,Integer> top1Candidates) {
        return JavaPairRDD.fromJavaRDD(top1Candidates
                .filter(x -> x != null)
                .filter(x -> x._1() != null && x._2() != null)
                .mapToPair(pair-> {
                    if (pair._1() <  pair._2()) { //put smaller entity id first
                        return new Tuple2<>(new Tuple2<>(pair._1(), pair._2()),1);
                    }
                    return new Tuple2<>(new Tuple2<>(pair._2(), pair._1()),1);                    
                })
                .reduceByKey((x,y)->x+y)                
                .filter(counts->counts._2() == 2) //1 from the first entity + 1 from the second
                .keys());
    }

    /**
     * Returns ALL reciprocal candidate matches, i.e., all candidate matches suggested from both collections.
     * @param topKValueCandidates
     * @param topKNeighborCandidates
     * @return 
     */
    public JavaPairRDD<Integer, IntArrayList> getReciprocalCandidateMatches(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates) {
        return JavaPairRDD.fromJavaRDD(topKValueCandidates                  
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear once or twice per entity)
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                
                .aggregateByKey(new IntOpenHashSet(), //discard duplicate candidates from values and neighbors for the same entity (per dataset)
                        (x,y) -> {x.addAll(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Byte>> outputPairs = new ArrayList<>(); //byte to save space (not expected to have values > 4) (pair, count)
                    if (keyId < 0) {
                        for (int candidate : pairs._2()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(keyId, candidate), (byte)1));
                        }
                    } else {
                        for (int candidate : pairs._2()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(candidate, keyId), (byte)1));
                        }
                    }                    
                    return outputPairs.iterator();
                })                
                .reduceByKey((x,y)-> (byte)(x+y))                      
                .filter(counts -> counts._2() > (byte)1) //can be 1 (non-reciprocal) or 2 (reciprocal)
                .keys())
                .aggregateByKey(new IntOpenHashSet(), 
                        (x,y) -> {x.add(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .mapValues(x-> new IntArrayList(x));
                
    }
    
    /**
     * sums the ranks, instead of similarity scores
     * @param topKValueCandidates
     * @param topKNeighborCandidates
     * @return 
     */
    public JavaPairRDD<Integer, Integer> getReciprocalMatches(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates) {
        
        //first heuristic
        JavaPairRDD<Integer,Integer> matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() < 0 && x._2().get(x._2().firstIntKey()) >= 1f) //keep pairs with negative key id and value_sim > 1
                .mapValues(x -> x.firstIntKey()); //return those pairs as matches
        
        System.out.println("Found "+matchesFromTop1Value.count()+" match suggestions from top-1 value sim > 1 from collection 2");    
        
        /*
        matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() >= 0 && x._2().get(x._2().firstIntKey()) >= 1f)
                .mapToPair(match -> new Tuple2<>(match._2().firstIntKey(), match._1()))
                .subtractByKey(matchesFromTop1Value) //only keep results that have not been already returned
                .union(matchesFromTop1Value);        
        System.out.println("Found "+matchesFromTop1Value.count()+" match suggestions from top-1 value sim > 1");                
        */
                
        float valueFactor = 0.6f;
        System.out.println("Value factor = "+valueFactor);
        
        topKValueCandidates = topKValueCandidates.subtractByKey(matchesFromTop1Value)
                .mapValues(x -> {
                    Int2FloatLinkedOpenHashMap scaledDownValues = new Int2FloatLinkedOpenHashMap();                    
                    int rank = x.size()+1;                                        
                    for (Map.Entry<Integer,Float> entry : x.entrySet()) {                        
                        rank--;
                        scaledDownValues.put(entry.getKey().intValue(), valueFactor*rank/x.size());                        
                    }                    
                    return scaledDownValues;
                });
                
        
        JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> candidatesWithAggregateScores = topKNeighborCandidates
                .mapValues(x -> {
                    Int2FloatLinkedOpenHashMap scaledDownValues = new Int2FloatLinkedOpenHashMap();                    
                    int rank = x.size()+1;                                        
                    for (Map.Entry<Integer,Float> entry : x.entrySet()) {                    
                        rank --;                            
                        scaledDownValues.put(entry.getKey().intValue(), (1-valueFactor)*rank/x.size());                    
                    }                    
                    return scaledDownValues;
                })
                .union(topKValueCandidates)                
                .aggregateByKey(new Int2FloatLinkedOpenHashMap(), //union semantics (sum the value and neighbor sim scores for the candidates of each collection)
                        (x,y) -> {
                            y.entrySet().stream().forEach(entry -> x.addTo(entry.getKey(), entry.getValue()));
                            return x;
                        }, 
                       (x,y) -> {
                            y.entrySet().stream().forEach(entry -> x.addTo(entry.getKey(), entry.getValue()));
                            return x;
                        }); 
        
        
        
        JavaPairRDD<Tuple2<Integer,Integer>, Float> edgesFromD1 = candidatesWithAggregateScores
                .filter(pair -> pair._1() >= 0)
                .flatMapToPair(pairs -> {                    
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //key:(-eId,+eID) value: sim_score (summed)                    
                    for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                        outputPairs.add(new Tuple2<>(new Tuple2<>(candidate.getKey(), pairs._1()), candidate.getValue()));
                    }
                    return outputPairs.iterator();
                });
        JavaPairRDD<Tuple2<Integer,Integer>, Float> edgesFromD2 = candidatesWithAggregateScores
                .filter(pair -> pair._1() < 0)
                .flatMapToPair(pairs -> {                    
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //key:(-eId,+eID) value: sim_score (summed)                    
                    for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                        outputPairs.add(new Tuple2<>(new Tuple2<>(pairs._1(), candidate.getKey()), candidate.getValue()));
                    }
                    return outputPairs.iterator();
                });
                
        JavaPairRDD<Tuple2<Integer,Integer>, Tuple2<Float,Float>> reciprocalEdges = edgesFromD1.join(edgesFromD2);//keep only reciprocal edges (suggested by both collections)
        
        /*
        //keep only reciprocal edges from the first heuristic (makes reciprocity the first heuristic)        
        matchesFromTop1Value = reciprocalEdges.mapToPair(x-> x._1()) //equivalent to keys(), but keys does not return JavaPairRDD
                .distinct()
                .reduceByKey((x,y) -> x) //dummy action to remove duplicate keys
                .join(matchesFromTop1Value)
                .mapValues(x -> x._2());
        System.out.println(matchesFromTop1Value.count()+" of them are reciprocal");                
        */
        
        /*
        //reciprocity second (or first, if previous block is un-commented)
        return reciprocalEdges
                .mapValues(x -> x._1()+x._2()) //just sum the scores from the first and the second collection for the same candidate pair (they are most likely equal)
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                                
                .subtractByKey(matchesFromTop1Value)  //for the rest, not examined yet...
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score                
                .mapValues(x-> x._1()) //keep candidate id only and lose the score
                .union(matchesFromTop1Value);
         */       
        
        
        /*
        //ignore reciprocity     (to evaluate H4)   
        return edgesFromD1.fullOuterJoin(edgesFromD2)
                .mapValues(x -> x._1().orElse(0f)+x._2().orElse(0f)) //just sum the scores from the first and the second collection for the same candidate pair (they are most likely equal)
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                                
                .subtractByKey(matchesFromTop1Value)  //for the rest, not examined yet...
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest score                
                .mapValues(x-> x._1()) //keep candidate id only and lose the score
                .union(matchesFromTop1Value); //TODO: un-comment for the final test
         */     
        
        
        
        //reciprocity last  
        JavaPairRDD<Integer,Iterable<Integer>> reciprocalEdgesPerEntity = reciprocalEdges.mapToPair(x-> x._1()) //equivalent to keys(), but keys() does not return JavaPairRDD
                .groupByKey();
        return edgesFromD1.fullOuterJoin(edgesFromD2)
                .mapValues(x -> x._1().orElse(0f)+x._2().orElse(0f)) //just sum the scores from the first and the second collection for the same candidate pair (they are most likely equal)
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                                
                .subtractByKey(matchesFromTop1Value)  //for the rest, not examined yet...
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score                                
                .mapValues(x-> x._1()) //keep candidate id only and lose the score
                .join(reciprocalEdgesPerEntity)
                .filter(x-> {
                    for (int reciprocalEdge : x._2()._2()) {
                        if (reciprocalEdge == x._2()._1()) {
                            return true;
                        }
                    }
                    return false;
                })
                .mapValues(x-> x._1())
                .union(matchesFromTop1Value);    
    }
}
