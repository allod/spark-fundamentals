package training.day3.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class StructuredNetworkWordCountJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Structured network word count java")
                .getOrCreate();


        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines.as(Encoders.STRING())
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();

        wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start()
                .awaitTermination();
    }
}
