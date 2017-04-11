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

import scala.Tuple2;


/**
 *
 * @author vefthym
 */
public class ComparableIntFloatPair implements Comparable<ComparableIntFloatPair>, java.io.Serializable {
    // public final fields ok for this small example
    private final int entityId;
    private final float value;    

    public ComparableIntFloatPair(Tuple2<Integer,Float> input) {
        this.entityId = input._1();
        this.value = input._2();        
    }
    
    public ComparableIntFloatPair(int entityId, float value) {
        this.entityId = entityId;
        this.value = value;        
    }
   
    public int getEntityId(){
        return entityId;
    }
    
    public float getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return entityId+":"+value;
    }

    @Override
    public int compareTo(ComparableIntFloatPair other) {
        return Float.compare(value, other.getValue()); //return the natural order result (for ascending sorting)        
    }
        
} 

