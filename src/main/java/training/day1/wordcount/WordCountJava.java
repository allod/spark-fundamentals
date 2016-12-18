package training.day1.wordcount;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Map;

import static training.Utils.DATA_DIRECTORY_PATH;

public class WordCountJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Word count java")
                .getOrCreate();

        //Create rdd for the text file
        JavaRDD<String> input = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Lazy transformation
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""));
        //Action that triggers computation
        Map<String, Long> wordCounts = words.countByValue();

        System.out.println("Word count map: " + wordCounts);
        System.out.println("Hatter count is " + wordCounts.get("hatter"));
    }
}
