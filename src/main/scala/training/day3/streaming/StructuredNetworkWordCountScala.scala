package training.day3.streaming

import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Structured network word count scala")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
