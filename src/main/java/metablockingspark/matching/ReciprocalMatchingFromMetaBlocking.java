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

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaPairRDD;
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

    public JavaPairRDD<Integer, IntArrayList> getReciprocalCandidateMatches(JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates) {
        return JavaPairRDD.fromJavaRDD(topKValueCandidates
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear one or twice per entity)
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
                .reduceByKey((x,y)-> (byte)(x+y))                      
                .filter(counts -> counts._2 > (byte)1)
                .keys())
                .aggregateByKey(new IntOpenHashSet(), 
                        (x,y) -> {x.add(y); return x;}, 
                        (x,y) -> {x.addAll(y); return x;})
                .mapValues(x-> new IntArrayList(x));
                
    }
    
    
    
    public JavaPairRDD<Integer, Integer> getReciprocalMatches(JavaPairRDD<Integer, Int2FloatOpenHashMap> topKValueCandidates, JavaPairRDD<Integer, IntArrayList> topKNeighborCandidates) {
        return topKValueCandidates
                .mapValues(x -> new IntArrayList(x.keySet())) //not sorted!                
                .union(topKNeighborCandidates) //bag semantics (a candidate match may appear one or twice per entity)
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
                .reduceByKey((x,y)-> (byte)(x+y))                      
                .mapToPair(candidates -> new Tuple2<>(candidates._1()._1(), new Tuple2<>(candidates._1()._2(), candidates._2()))) //(-Id,(+id,recipr.score))
                .reduceByKey((x,y) -> x._2() > y._2() ? x : y) //keep the candidate with the highest reciprocal score per entity
                .mapValues(candidate -> candidate._1()); //keep only the id
    }
}
