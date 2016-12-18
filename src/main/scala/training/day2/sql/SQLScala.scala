package training.day2.sql

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object SQLScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SQL example scala")
      .getOrCreate()

    //load emails data to DataFrame
    val emails = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "item")
      .load(DATA_DIRECTORY_PATH + "emails.xml")

    //print emails schema
    emails.printSchema

    //register DataFrame as table
    emails.createOrReplaceTempView("emails")

    //describe table
    spark.sql("describe emails").show

    //Write query to get all unique location values from emails
    spark.sql("select distinct location from emails").show

    //Write query to get all unique keyword values from top level `keyword` column
    //hint: use `explode` function to flatten array elements
    spark.sql("select explode(keyword) as keyword from emails").show()

    //Create table `shopping` based on `emails` table data with only `name`, `payment`, `quantity` and `shipping` columns
    spark.sql("select name, payment, quantity, shipping from emails").createOrReplaceTempView("shopping")

    //Select records from `shopping` table where `quantity` is greater then 1
    spark.sql("select * from shopping where quantity > 1").show()

    //Create table 'shipping_dates' that contains all `date` values from the `mail` top level column
    //hint: create intermediate dataFrames or tables to handle nesting levels
    spark.sql("select explode(mail) as element from emails")
      .select("element.date")
      .createOrReplaceTempView("shipping_dates")

    //Register custom user defined function to parse date string with format MM/DD/YYYY into java.sql.Date type
    spark.udf.register("parseDate", (string: String) => {
      val simpleDateFormat = new SimpleDateFormat("dd/mm/yyyy")
      val date = simpleDateFormat.parse(string)

      new java.sql.Date(date.getTime)
    })

    //Select unique and sorted records from `shipping_dates` table
    //hint: use `parseDate` udf to get correct sorting
    spark.sql("select date from shipping_dates")
      .selectExpr("parseDate(date) as date")
      .distinct
      .orderBy("date")
      .show()

    //Save `emails`, `shopping` and 'shipping_dates' table to json, csv and text files accordingly
    spark.sql("select * from emails").write.json(DATA_DIRECTORY_PATH + "emails.json")
    spark.sql("select * from shopping").write.csv(DATA_DIRECTORY_PATH + "shopping.csv")
    spark.sql("select * from shipping_dates").write.text(DATA_DIRECTORY_PATH + "shipping_dates.text")
  }
}
