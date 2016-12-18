package training.day2.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

import static training.Utils.DATA_DIRECTORY_PATH;

public class SQLJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SQL example java")
                .getOrCreate();

        //load emails data to DataFrame
        Dataset<Row> emails = spark.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "item")
                .load(DATA_DIRECTORY_PATH + "emails.xml");

        //print emails schema
        emails.printSchema();

        //register DataFrame as table
        emails.createOrReplaceTempView("emails");

        //describe table
        spark.sql("describe emails").show();

        //TODO
        //Write query to get all unique location values from emails
        spark.sql("");

        //TODO
        //Write query to get all unique keyword values from top level `keyword` column
        //hint: use `explode` function to flatten array elements
        spark.sql("");

        //TODO
        //Create table `shopping` based on `emails` table data with only `name`, `payment`, `quantity` and `shipping` columns
        spark.sql("");

        //TODO
        //Select records from `shopping` table where `quantity` is greater then 1
        spark.sql("");

        //TODO
        //Create table 'shipping_dates' that contains all `date` values from the `mail` top level column
        //hint: create intermediate dataFrames or tables to handle nesting levels
        spark.sql("");

        //Register custom user defined function to parse date string with format MM/DD/YYYY into java.sql.Date type
        UDF1<String, java.sql.Date> udf = (String string) -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/mm/yyyy");
            Date date = simpleDateFormat.parse(string);

            return new java.sql.Date(date.getTime());
        };

        spark.udf().register("parseDate", udf, DataTypes.DateType);

        //TODO
        //Select unique and sorted records from `shipping_dates` table
        //hint: use `parseDate` udf to get correct sorting
        spark.sql("");

        //TODO
        //Save `emails`, `shopping` and 'shipping_dates' table to json, csv and text files accordingly
    }
}
