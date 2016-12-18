package training.day1.accumulators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static training.Utils.DATA_DIRECTORY_PATH;

public class BrokenAliceCountJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Broken Alice counter java")
                .getOrCreate();

        //Create rdd for the text file
        JavaRDD<String> input = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Attempt to create counter
        AtomicInteger brokenCounter = new AtomicInteger(0);

        //Transforming text to words and trying to count alice words on the way
        JavaRDD<String> words = input
                .flatMap(word -> Arrays.asList(word.split(" ")).iterator())
                .map(word -> {
                    String cleanWord = word.toLowerCase().replaceAll("[^a-z]", "");
                    if (cleanWord.equals("alice")) {
                        brokenCounter.incrementAndGet();
                    }
                    return cleanWord;
                });

        //Action that triggers computation
        Map<String, Long> wordCounts = words.countByValue();

        System.out.println("Count of word 'Alice' using broken counter: " + brokenCounter);
        System.out.println("Actual count of word 'Alice': " + wordCounts.get("alice"));
    }
}
