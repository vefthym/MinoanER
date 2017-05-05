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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import metablockingspark.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author vefthym
 */
public class LabelMatchingHeuristic {
    
    /**
     * Returns the pairs that follow the following conditions:
     * 1. the entities of this pair have the same label
     * 2. only this pair of entities has this label
     * @param inputTriples1
     * @param inputTriples2
     * @param entityIds1
     * @param entityIds2
     * @param SEPARATOR
     * @return 
     */
    public JavaPairRDD<Integer,Integer> getMatchesFromLabels(JavaRDD<String> inputTriples1, JavaRDD<String> inputTriples2, JavaRDD<String> entityIds1, JavaRDD<String> entityIds2, String SEPARATOR, Set<String> labelAtts1, Set<String> labelAtts2) {                
        JavaPairRDD<String,Integer> labelBlocks1 = getLabelBlocks(inputTriples1, labelAtts1, entityIds1, SEPARATOR, true);
        JavaPairRDD<String,Integer> labelBlocks2 = getLabelBlocks(inputTriples2, labelAtts2, entityIds2, SEPARATOR, false);
        
        return labelBlocks2.join(labelBlocks1) //get blocks from labels existing in both collections (inner join) (first D2, to keep negative ids first)
                .reduceByKey((x,y) -> null) //if the block has more than two (one pair of) entities, skip this block
                .filter(x-> x._2() != null)
                .mapToPair(pair -> new Tuple2<>(pair._2()._1(), pair._2()._2()))
                .distinct()
                .reduceByKey((x,y) -> null) //if the entity is matched to more than one entities, skip this entity (this could happen when this entity has > 1 labels, one of them same with one entity, and the other same with another entity)
                .filter(x-> x._2() != null);
                
    }
    
    private JavaPairRDD<String,Integer> getLabelBlocks(JavaRDD<String> inputTriples, Set<String> labelAtts, JavaRDD<String> entityIds, String SEPARATOR, boolean positiveIds) {
        Object2IntOpenHashMap<String> urls1 = Utils.readEntityIdsMapping(entityIds, positiveIds);
        return inputTriples.mapToPair(line -> {
        String[] spo = line.toLowerCase().replaceAll(" \\.$", "").split(SEPARATOR); //lose the ending " ." from valid .nt files
          if (spo.length < 3) {
              return null;
          }          
          if (labelAtts.contains(spo[1])) {
            String labelValue = line.substring(line.indexOf(spo[1])+spo[1].length()+SEPARATOR.length())
                    .toLowerCase().replaceAll("[^a-z0-9 ]", "").trim();
            int subjectId = urls1.getInt(Utils.encodeURIinUTF8(spo[0])); //replace subject url with entity id
            if (!positiveIds) {
                subjectId = -subjectId;
            }
            return new Tuple2<String,Integer>(labelValue,subjectId);
          } else {
              return null;
          }          
        })
        .filter(x-> x!= null);
    }
    
}
