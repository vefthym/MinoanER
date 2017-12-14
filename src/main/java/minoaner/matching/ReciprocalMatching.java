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
package minoaner.matching;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class ReciprocalMatching {
    
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
}
