package training.day1.pi;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Math.random;
import static java.util.stream.Collectors.toList;

public class SparkPiJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Spark Pi java")
                .getOrCreate();

        List<Integer> range = IntStream.range(1, 100000).boxed().collect(toList());
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        int count = javaSparkContext
                .parallelize(range)
                .map(i -> {
                    double x = random() * 2 - 1;
                    double y = random() * 2 - 1;
                    return x * x + y * y < 1 ? 1 : 0;
                }).reduce((sum1, sum2) -> sum1 + sum2);

        System.out.println("Pi is roughly " + 4.0 * count / (100000 - 1));
    }
}
