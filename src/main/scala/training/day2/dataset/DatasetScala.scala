package training.day2.dataset

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

    //TODO
    //Print distinct three word counties from zipCode dataset
    zipCodeDS.show()

    //TODO
    //Print most popular names in person dataset
    personDS.show()

    //TODO
    //Print number of people by state
    zipCodeDS.join(personDS, "zip")

    //TODO
    //Save to json file cities that have more then five companies
    zipCodeDS.join(personDS, "zip")
  }
}
