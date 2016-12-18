package training.day1.accumulators


import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object BrokenAliceCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Broken Alice counter scala")
      .getOrCreate()

    //Attempt to create counter
    val brokenCounter = new AtomicInteger(0)
    //Create rdd for the text file
    val input = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")

    //Transforming text to words and trying to count alice words on the way
    val words = input.flatMap(x => x.split(" ")).map(word => {
      val cleanWord = word.replaceAll("[^A-Za-z]", "")
      if (cleanWord == "Alice") brokenCounter.incrementAndGet()
      cleanWord
    })

    //Action that triggers computation
    val wordCounts = words.countByValue()

    println("Count of word 'Alice' using broken counter: " + brokenCounter)
    println("Actual count of word 'Alice': " + wordCounts("Alice"))
  }
}
