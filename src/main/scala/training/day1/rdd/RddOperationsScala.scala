package training.day1.rdd

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object RddOperationsScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("RDD operations scala")
      .getOrCreate()

    val text = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")

    //Lets count number of non empty lines
    val numberOfNonEmptyLines = text.filter(line => !line.isEmpty).count
    println(s"There are $numberOfNonEmptyLines non empty lines")

    //Find what is the most frequent word length in text
    val mostFrequentWordLength: Integer = text.flatMap(line => line.split(" "))
      .map(word => word.toLowerCase().replaceAll("[^a-z]", ""))
      .keyBy(word => word.length())
      .aggregateByKey(0)((count, word) => count + 1, (count1, count2) => count1 + count2)
      .map(tuple => tuple.swap)
      .sortByKey(ascending = false)
      .values
      .first()

    System.out.println(s"Most frequent word length in text is $mostFrequentWordLength")

    //Print all distinct words for the most frequent word length
    val words = text.flatMap(line => line.split(" "))
      .map(word => word.toLowerCase().replaceAll("[^a-z]", ""))
      .filter(word => word.length() == mostFrequentWordLength)
      .collect

    System.out.println(s"Print all distinct words for the most frequent word length: ${words.mkString(", ")}")
  }
}
