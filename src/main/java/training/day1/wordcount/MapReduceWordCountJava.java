package training.day1.wordcount;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

import static training.Utils.DATA_DIRECTORY_PATH;

/*
 Map Reduce word count algorithm example

 1. Given an RDD of words

 +--p1--+
 |Alice |
 |rabbit|
 |little|
 |Alice |
 |clock |
 |Alice |
 |Alice |
 |white |
 +------+
 +--p2--+
 |flower|
 |cat   |
 |rabbit|
 |Alice |
 |rabbit|
 +------+
 +--p3--+
 |Hatter|
 |tea   |
 |Alice |
 |rabbit|
 |white |
 +------+

 2. Transform it to following rdd using variation of `map` function

 +---p1---+
 |Alice  1|
 |rabbit 1|
 |little 1|
 |Alice  1|
 |clock  1|
 |Alice  1|
 |Alice  1|
 |white  1|
 +--------+
 +---p2---+
 |flower 1|
 |cat    1|
 |rabbit 1|
 |Alice  1|
 |rabbit 1|
 +--------+
 +---p3---+
 |Hatter 1|
 |tea    1|
 |Alice  1|
 |rabbit 1|
 |white  1|
 +--------+


 3. Sum up all occurrences by word using variation of `reduce` function

 3.1 First sum up is performed inside partition

 +---p1---+
 |Alice  4|
 |rabbit 1|
 |little 1|
 |clock  1|
 |white  1|
 +--------+
 +---p2---+
 |flower 1|
 |cat    1|
 |rabbit 2|
 |Alice  1|
 +--------+
 +---p3---+
 |Hatter 1|
 |tea    1|
 |Alice  1|
 |rabbit 1|
 |white  1|
 +--------+

 3.2 Then sum up is done for data from all partitions

 +--res---+
 |Alice  6|
 |rabbit 4|
 |little 1|
 |clock  1|
 |white  2|
 |flower 1|
 |cat    1|
 |Hatter 1|
 |tea    1|
 +--------+

 */
public class MapReduceWordCountJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Map reduce word count java")
                .getOrCreate();

        //Create rdd for the text file
        JavaRDD<String> input = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Lazy transformation
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""));

        Map<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((partialSum1, partialSum2) -> partialSum1 + partialSum2)
                .collectAsMap();

        System.out.println("Word count map: " + wordCounts);
        System.out.println("Hatter count is " + wordCounts.get("hatter"));
    }
}
