package training.day2.dataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import training.Utils.DATA_DIRECTORY_PATH

object DatasetScala {

  case class Person(firstName: String, lastName: String, companyName: String, zip: Int, email: String)

  case class ZipCode(zip: Int, city: String, county: String, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataset example scala")
      .getOrCreate()

    import spark.implicits._

    val zipTablePath = DATA_DIRECTORY_PATH + "zip.csv"
    val personTablePath = DATA_DIRECTORY_PATH + "persons.parquet"

    val zipCodeDS: Dataset[ZipCode] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zipTablePath)
      .as[ZipCode]

    //Print zipCode schema
    zipCodeDS.printSchema

    //Print first 20 rows from zipCode dataset
    zipCodeDS.show

    val personDS: Dataset[Person] = spark.read
      .parquet(personTablePath)
      .as[Person]

    //Print person schema
    personDS.printSchema

    //Print first 20 rows from person dataset
    personDS.show()

    val joinCondition = personDS("zip") === zipCodeDS("zip")
    val joined = personDS.join(zipCodeDS, joinCondition)
    val personInfo = joined.select($"firstName", $"lastName", $"city")

    //Print first 20 rows from joined dataset
    personInfo.show()

    //Print distinct three word counties from zipCode dataset
    zipCodeDS.select("county")
      .where("county like '% % %'")
      .distinct
      .show()

    //Print most popular names in person dataset
    personDS.groupBy("firstName")
      .agg(count("*").as("count"))
      .sort(desc("count"))
      .show()

    //Print number of people by state
    zipCodeDS.join(personDS, "zip")
      .groupBy("state")
      .agg(count("*"))
      .show()

    //Save to json file cities that have more then five companies
    zipCodeDS.join(personDS, "zip")
      .groupBy("city")
      .agg(collect_list("companyName").as("companyNames"))
      .where(size(col("companyNames")) > 5)
      .coalesce(1)
      .write
      .json(DATA_DIRECTORY_PATH + "cities.json")
  }
}
