package training.day3.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class NetworkWordCountJava {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Network word count java")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(3));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER());
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Long> wordCounts = words.countByValue();
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
