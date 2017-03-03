package metablockingspark.preprocessing;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author vefthym
 */
public class BlocksPerEntity {
    
    SparkSession spark;
    JavaPairRDD<Integer,Integer[]> entityIndex;

    public BlocksPerEntity(SparkSession spark, JavaPairRDD<Integer, Integer[]> entityIndex) {
        this.spark = spark;
        this.entityIndex = entityIndex;
    }
    
    public JavaPairRDD<Integer,Integer> run() {
        return entityIndex.mapValues(x -> x.length);
    }    
}
