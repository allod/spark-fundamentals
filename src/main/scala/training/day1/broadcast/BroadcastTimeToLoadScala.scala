package training.day1.broadcast

import org.apache.spark.sql.SparkSession

object BroadcastTimeToLoadScala {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Broadcast loading time scala")
      .master("local")
      .getOrCreate()

    val data = (0 until 1000000).toList
    val broadcast = spark.sparkContext.broadcast(data)

    for (i <- 0 until 3) {
      println("===========")
      println("Iteration " + i)
      val startTime = System.nanoTime
      val observedSizes = spark.sparkContext.parallelize(1 to 10).map(_ => broadcast.value.length)

      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }
  }
}
