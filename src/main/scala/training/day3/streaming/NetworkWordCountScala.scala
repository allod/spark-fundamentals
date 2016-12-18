package training.day3.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Network word count scala")
      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(3))

    val lines = streamingContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.countByValue()
    wordCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
