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
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class ReciprocalMatchingFromMetaBlocking {
    
    public JavaPairRDD<Integer,Integer> getReciprocalMatches(JavaPairRDD<Integer,Integer> top1Candidates) {
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
    
    
    
    public JavaPairRDD<Integer, Integer> getReciprocalMatches(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates) {
        return topKValueCandidates
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear once or twice per entity)
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                                
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Byte>> outputPairs = new ArrayList<>(); //byte to save space (not expected to have values > 4)
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
                .reduceByKey((x,y)-> (byte)(x+y)) //reciprocity is here -> the possible values are 1, 2, 3, 4          
                .filter(x-> x._2() > (byte)1) //reciprocity requirement
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score per entity
                .mapValues(candidate -> candidate._1()); //keep only the id
    }
    
    
    
    public JavaPairRDD<Integer, Integer> getReciprocalMatchesTEST(JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates, LongAccumulator ties, LongAccumulator tiesAbove1) {
        return topKValueCandidates
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear once or twice per entity)
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Byte>> outputPairs = new ArrayList<>(); //byte to save space (not expected to have values > 4)
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
                .reduceByKey((x,y)-> (byte)(x+y)) //reciprocity is here -> the possible values are 1, 2, 3, 4          
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                
                .groupByKey()
                .mapValues(x -> {
                    byte max = (byte) 0;
                    for (Tuple2<Integer,Byte> candidate : x) {
                        if (candidate._2() > max) {
                            max = candidate._2();
                        }
                    }       
                    if (max == 1) {
                        return null;
                    }
                    int winner = 0;
                    boolean foundMoreThanOne = false;
                    for (Tuple2<Integer,Byte> candidate : x) {
                        if (candidate._2() == max) {                                                        
                            winner = candidate._1();
                            if (foundMoreThanOne) {
                                ties.add(1);                                
                                if (max > (byte)1) {
                                    tiesAbove1.add(1);
                                } 
                                return winner; 
                            } else {
                                foundMoreThanOne = true;
                            }
                        }
                    }
                    return winner;
                })
                .filter (x -> x._2() != null);
                
    }
    
    
    
    
    
    
    public JavaPairRDD<Integer, IntArrayList> getReciprocalMatchesTEST2(JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates, LongAccumulator ties, LongAccumulator tiesAbove1) {
        return topKValueCandidates
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear once or twice per entity)
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Byte>> outputPairs = new ArrayList<>(); //byte to save space (not expected to have values > 4)
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
                .reduceByKey((x,y)-> (byte)(x+y)) //reciprocity is here -> the possible values are 1, 2, 3, 4          
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                
                .groupByKey()
                .mapValues(x -> {
                    byte max = (byte) 0;
                    for (Tuple2<Integer,Byte> candidate : x) {
                        if (candidate._2() > max) {
                            max = candidate._2();
                        }
                    }       
                    /*
                    if (max == 1) { //see how many matches have score 4
                        return null;
                    } 
                    */
                    IntArrayList winners = new IntArrayList();
                    for (Tuple2<Integer,Byte> candidate : x) {
                        if (candidate._2() == max) {                                                                                    
                            winners.add(candidate._1());                            
                        }
                    }
                    return winners;
                })
                .filter (x -> x._2() != null);
                
    }
    
    
    
    
    public JavaPairRDD<Integer, IntArrayList> getReciprocalMatchesTEST3(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates, LongAccumulator ties, LongAccumulator tiesAbove1) {
        return topKValueCandidates                                
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear once or twice per entity)
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //byte to save space (not expected to have values > 4)
                    if (keyId < 0) {
                        for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(keyId, candidate.getKey()), candidate.getValue()));
                        }
                    } else {
                        for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(candidate.getKey(), keyId), candidate.getValue()));
                        }
                    }                    
                    return outputPairs.iterator();
                })                
                .reduceByKey((x,y)-> x+y) //aggregate the values for the same entity pair (met max 4 times)
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                
                .groupByKey()
                .mapValues(x -> {
                    float max = 0f;
                    for (Tuple2<Integer,Float> candidate : x) {
                        if (candidate._2() > max) {
                            max = candidate._2();
                        }
                    }       
                    /*
                    if (max == 1) { //see how many matches have score 4
                        return null;
                    } 
                    */
                    IntArrayList winners = new IntArrayList();
                    for (Tuple2<Integer,Float> candidate : x) {
                        if (candidate._2() == max) {                                                                                    
                            winners.add(candidate._1());                            
                        }
                    }
                    return winners;
                })
                .filter (x -> x._2() != null);
                
    }
    
    
    public JavaPairRDD<Integer, Integer> getReciprocalMatchesTEST4(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates, LongAccumulator ties, LongAccumulator tiesAbove1) {
        
        JavaPairRDD<Integer,Integer> matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() < 0 && x._2().get(x._2().firstIntKey()) >= 1f) //keep pairs with negative key id and value_sim > 1
                .mapValues(x -> x.firstIntKey()); //return those pairs as matches //todo: check for ties at first place with scores > 1
        
        /*
        //do the same for positive key ids
        matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() >= 0 && x._2().get(x._2().firstIntKey()) >= 1f)
                .mapToPair(match -> new Tuple2<>(match._2().firstIntKey(), match._1()))
                .subtractByKey(matchesFromTop1Value)
                .union(matchesFromTop1Value);
        */
        
        System.out.println("Found "+matchesFromTop1Value.count()+" match suggestions from top-1 value sim > 1");        
        
        return topKNeighborCandidates
                .mapValues(x -> {
                    Int2FloatLinkedOpenHashMap scaledDownValues = new Int2FloatLinkedOpenHashMap();
                    float scaleFactor  = 0.1f;
                    for (Map.Entry<Integer, Float> entry : x.entrySet()) {
                        scaledDownValues.put(entry.getKey().intValue(), entry.getValue()*scaleFactor);
                    }
                    return scaledDownValues;
                })
                .union(topKValueCandidates) //bag semantics (a candidate match may appear once or twice per entity)                                                
                .flatMapToPair(pairs -> {
                    int keyId = pairs._1();
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //key:(-eId,+eID) value: sim_score (summed)
                    if (keyId < 0) {
                        for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(keyId, candidate.getKey()), candidate.getValue()));
                        }
                    } else {
                        float scaleFactor = 1.0f;
                        for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                            outputPairs.add(new Tuple2<>(new Tuple2<>(candidate.getKey(), keyId), candidate.getValue()*scaleFactor)); //less important than the other dataset
                        }
                    }                    
                    return outputPairs.iterator();
                })                     
                .reduceByKey((x,y)-> x+y) //aggregate the values for the same entity pair (met max 4 times, 1 from values & 1 from neighbors, for each entity of the pair)                
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                                
                .subtractByKey(matchesFromTop1Value)  //for the rest, not examined yet...
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score
                .mapValues(x-> x._1()) //keep candidate id only and lose the score
                .union(matchesFromTop1Value);
                
    }
    
    
    
    
    public JavaPairRDD<Integer, Integer> getReciprocalMatchesTEST5(JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, Int2FloatLinkedOpenHashMap> topKNeighborCandidates, LongAccumulator ties, LongAccumulator tiesAbove1) {
        
        JavaPairRDD<Integer,Integer> matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() < 0 && x._2().get(x._2().firstIntKey()) >= 1f) //keep pairs with negative key id and value_sim > 1
                .mapValues(x -> x.firstIntKey()); //return those pairs as matches //todo: check for ties at first place with scores > 1
        
        System.out.println("Found "+matchesFromTop1Value.count()+" match suggestions from top-1 value sim > 1 from collection 2");
        /*
        //do the same for positive key ids
        matchesFromTop1Value = topKValueCandidates
                .filter(x -> x._1() >= 0 && x._2().get(x._2().firstIntKey()) >= 1f)
                .mapToPair(match -> new Tuple2<>(match._2().firstIntKey(), match._1()))
                .subtractByKey(matchesFromTop1Value)
                .union(matchesFromTop1Value);        
        
        System.out.println("Found "+matchesFromTop1Value.count()+" match suggestions from top-1 value sim > 1");        
        */
        
        JavaPairRDD<Integer,Int2FloatLinkedOpenHashMap> candidatesWithAggregateScores = topKNeighborCandidates
                .mapValues(x -> {
                    Int2FloatLinkedOpenHashMap scaledDownValues = new Int2FloatLinkedOpenHashMap();
                    float scaleFactor  = 0.1f;
                    x.entrySet().stream().forEach(entry -> scaledDownValues.put(entry.getKey().intValue(), entry.getValue()*scaleFactor));
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
        
        JavaPairRDD<Tuple2<Integer,Integer>, Float> d1Candidates = candidatesWithAggregateScores
                .filter(pair -> pair._1() >= 0)
                .flatMapToPair(pairs -> {                    
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //key:(-eId,+eID) value: sim_score (summed)                    
                    for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                        outputPairs.add(new Tuple2<>(new Tuple2<>(candidate.getKey(), pairs._1()), candidate.getValue()));
                    }
                    return outputPairs.iterator();
                });
        JavaPairRDD<Tuple2<Integer,Integer>, Float> d2Candidates = candidatesWithAggregateScores
                .filter(pair -> pair._1() < 0)
                .flatMapToPair(pairs -> {                    
                    List<Tuple2<Tuple2<Integer,Integer>, Float>> outputPairs = new ArrayList<>(); //key:(-eId,+eID) value: sim_score (summed)                    
                    for (Map.Entry<Integer,Float> candidate : pairs._2().entrySet()) { //a candidate may be checked twice (on purpose)
                        outputPairs.add(new Tuple2<>(new Tuple2<>(pairs._1(), candidate.getKey()), candidate.getValue()));
                    }
                    return outputPairs.iterator();
                });
        
        return d1Candidates.join(d2Candidates) //keep only reciprocal candidates
                .mapValues(x -> x._1()+x._2()) //just sum the scores from the first and the second collection for the same candidate pair (they are most likely equal)
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))                                                
                .subtractByKey(matchesFromTop1Value)  //for the rest, not examined yet...
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score
                .mapValues(x-> x._1()) //keep candidate id only and lose the score
                .union(matchesFromTop1Value);
                
    }
}
