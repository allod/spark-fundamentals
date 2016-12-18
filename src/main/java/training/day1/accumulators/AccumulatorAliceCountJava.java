package training.day1.accumulators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.Map;

import static training.Utils.DATA_DIRECTORY_PATH;

public class AccumulatorAliceCountJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Accumulator Alice counter java")
                .getOrCreate();

        //Create rdd for the text file
        JavaRDD<String> input = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Create counter using spark accumulator
        LongAccumulator counter = spark.sparkContext().longAccumulator("aliceCounter");

        //Transforming text to words and counting alice words on the way
        JavaRDD<String> words = input
                .flatMap(word -> Arrays.asList(word.split(" ")).iterator())
                .map(word -> {
                    String cleanWord = word.toLowerCase().replaceAll("[^a-z]", "");
                    if (cleanWord.equals("alice")) {
                        counter.add(1);
                    }
                    return cleanWord;
                });

        //Action that triggers computation
        Map<String, Long> wordCounts = words.countByValue();

        System.out.println("Count of word 'Alice' using accumulator: " + counter.value());
        System.out.println("Actual count of word 'Alice': " + wordCounts.get("alice"));
    }
}
