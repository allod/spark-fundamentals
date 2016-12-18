package training.day1.rdd

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object RddGroupingAndJoiningScala {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDD grouping and joining java")
      .getOrCreate

    val personRdd = spark.sparkContext
      .textFile(DATA_DIRECTORY_PATH + "persons.csv")
      .map(line => {
        val columns = line.split(",")
        Person(columns(0), columns(1), columns(2))
      })

    val zipCodeRdd = spark.sparkContext
      .textFile(DATA_DIRECTORY_PATH + "zip.csv")
      .map(line => {
        val columns = line.split(",")
        (columns(1), columns(0))
      })

    val groupedByCity = personRdd.groupBy(person => person.city).collectAsMap
    for ((city, persons) <- groupedByCity) {
      println(s"$city persons: $persons")
    }

    val personPairRDD = personRdd.keyBy(_.city)

    val joined = personPairRDD.join(zipCodeRdd)
    val personToZipCode = joined.values.collectAsMap
    for ((person, zipCode) <- personToZipCode) {
      println(s"$person zip code: $zipCode")
    }
  }

  case class Person(firstName: String, lastName: String, city: String)

}
