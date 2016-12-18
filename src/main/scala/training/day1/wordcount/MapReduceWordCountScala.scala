package training.day1.wordcount

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

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
object MapReduceWordCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Map reduce word count scala")
      .getOrCreate()

    //Create rdd for the text file
    val input = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")
    //Lazy transformation
    val words = input.flatMap(x => x.split(" "))

    val wordCounts = words.map(word => (word, 1))
      .reduceByKey((partialSum1, partialSum2) => partialSum1 + partialSum2)
      .collectAsMap()

    println(s"Word count map: $wordCounts")
    println(s"Hatter count is ${wordCounts("Hatter")}")
  }
}
