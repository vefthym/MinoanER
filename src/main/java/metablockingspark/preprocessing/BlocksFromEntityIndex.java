package metablockingspark.preprocessing;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 * Reconstructs a blocking collection from an input entity index.
 * The difference in this output and the initial blocking collection, is that
 * block filtering has been applied.
 * @author vefthym
 */
public class BlocksFromEntityIndex {
    
    SparkSession spark;
    String blocksFromEIOutputPath;
    JavaPairRDD<Integer,Integer[]> entityIndex;
    LongAccumulator cleanBlocksAccum, numComparisons;

    public BlocksFromEntityIndex(SparkSession spark, String entityIndexInputPath, String blocksFromEIOutputPath) {
        this.spark = spark;        
        this.blocksFromEIOutputPath = blocksFromEIOutputPath;
        this.entityIndex = JavaPairRDD.fromJavaRDD(JavaSparkContext.fromSparkContext(spark.sparkContext()).objectFile(entityIndexInputPath));
        cleanBlocksAccum = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
        numComparisons = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
    }
    
    public BlocksFromEntityIndex(SparkSession spark, JavaPairRDD<Integer,Integer[]> entityIndex, String blocksFromEIOutputPath) {
        this.spark = spark;        
        this.entityIndex = entityIndex;
        this.blocksFromEIOutputPath = blocksFromEIOutputPath;
        cleanBlocksAccum = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
        numComparisons = JavaSparkContext.fromSparkContext(spark.sparkContext()).sc().longAccumulator();
    }
    
    public JavaPairRDD<Integer, Iterable<Integer>> run() {
        return entityIndex.flatMapToPair(x -> 
            {
                List<Tuple2<Integer,Integer>> mapResults = new ArrayList<>();
                for (int blockId : x._2()) {
                    mapResults.add(new Tuple2<>(blockId, x._1()));
                }
                return mapResults.iterator();
            })
            .groupByKey()            
            .filter(x -> { //keep only blocks with > 2 entities and with entities from both datasets (i.e., at least 1 positive and 1 negative entityId)
                Iterable<Integer> entities = (Iterable<Integer>) x._2();
                long negatives = 0;
                boolean containsPositive = false;
                boolean containsNegative = false;
                long numEntities = entities.spliterator().getExactSizeIfKnown();
                if (numEntities < 2) {
                    return false;
                }
                for (int entityId : entities) {
                    if (entityId < 0) {
                        containsNegative = true;
                        negatives++;
                    } else {
                        containsPositive = true;
                    }
                }
                if (containsNegative && containsPositive) {
                    cleanBlocksAccum.add(1);
                    numComparisons.add(negatives * (numEntities-negatives));                
                    return true;
                }
                return false;
            });              
    }
    
}
