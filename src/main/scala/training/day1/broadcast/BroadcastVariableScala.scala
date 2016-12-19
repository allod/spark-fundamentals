package training.day1.broadcast

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object BroadcastVariableScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Broadcast variable scala")
      .getOrCreate()

    val input = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")

    val mainCharacters = Seq("Alice", "Hatter", "Rabbit")
    //TODO
    //Create broadcast variable for mainCharacters using sparkContext object
    val broadcast = null

    //Filter line that contain at least one main character
    val linesRdd = input.filter(line => /* implement me */ false)
    val lines = linesRdd.collect

    lines.foreach(println)
  }
}
