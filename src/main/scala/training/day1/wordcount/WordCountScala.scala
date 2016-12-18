package training.day1.wordcount

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Word count scala")
      .getOrCreate()

    //Create rdd for the text file
    val input = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")
    //Lazy transformation
    val words = input.flatMap(x => x.split(" "))
    //Action that triggers computation
    val wordCounts = words.countByValue()

    println(s"Word count map: $wordCounts")
    println(s"Hatter count is ${wordCounts("Hatter")}")
  }
}
