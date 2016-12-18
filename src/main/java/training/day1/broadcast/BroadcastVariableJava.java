package training.day1.broadcast;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static training.Utils.DATA_DIRECTORY_PATH;

public class BroadcastVariableJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Broadcast variable java")
                .getOrCreate();

        //Create java wrapper instance of spark context
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> input = sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt");

        List<String> mainCharacters = Arrays.asList("Alice", "Hatter", "Rabbit");
        Broadcast<List<String>> broadcast = sparkContext.broadcast(mainCharacters);

        JavaRDD<String> linesRdd = input.filter(line -> {
            Stream<String> characterStream = broadcast.getValue().stream();
            return characterStream.anyMatch(name -> line.contains(name));
        });

        List<String> lines = linesRdd.collect();
        lines.forEach(System.out::println);
    }
}
