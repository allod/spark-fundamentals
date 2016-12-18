package training.day1.pi

import org.apache.spark.sql.SparkSession

import scala.math.random

object SparkPiScala {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark Pi scala")
      .getOrCreate()

    val count = spark.sparkContext.parallelize(1 until 100000).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (100000 - 1))
  }
}
