package training.day1.broadcast;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class BroadcastTimeToLoadJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Broadcast loading time java")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        List<Integer> broadcastData = IntStream.range(0, 1000000).boxed().collect(toList());
        Broadcast<List<Integer>> broadcast = javaSparkContext.broadcast(broadcastData);

        for (int i = 0; i < 3; i++) {
            System.out.println("===========");
            System.out.println("Iteration " + i);
            long startTime = System.nanoTime();

            JavaRDD<String> rdd = javaSparkContext.parallelize(Collections.nCopies(10, "element"));

            // Collect the small RDD so we can print the observed sizes locally.
            rdd.map(o -> broadcast.value().size()).collect().forEach(size -> System.out.println(size));
            System.out.println(String.format("Iteration %d took %.0f milliseconds", i, (System.nanoTime() - startTime) / 1E6));
        }
    }
}
