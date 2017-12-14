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
package minoaner.utils;

/**
 *
 * @author vefthym
 */
public class ComparableIntFloatPairDUMMY extends ComparableIntFloatPair {
    public enum TYPE {VALUES, NEIGHBORS, BOTH, VALUES_WITH_EMPTY_NEIGHBORS, NEIGHBORS_WITH_EMPTY_VALUES};
    private final TYPE type;
    
    public ComparableIntFloatPairDUMMY(int entityId, float value, TYPE type) {
        super(entityId, value);
        this.type = type;
    }
    
    public TYPE getType() {
        return type;
    }
    
    @Override
    public String toString() {
        return super.toString()+":"+type;
    }   
} 

