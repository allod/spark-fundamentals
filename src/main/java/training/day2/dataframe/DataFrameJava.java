package training.day2.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static training.Utils.DATA_DIRECTORY_PATH;

public class DataFrameJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("DataFrame example java")
                .getOrCreate();

        String personsJsonTablePath = DATA_DIRECTORY_PATH + "persons.json";
        Dataset<Row> personDF = spark.read().json(personsJsonTablePath);

        //Print personDF schema
        personDF.printSchema();

        //TODO
        //Print 100 persons from personDF dataFrame
        personDF.show();

        //TODO
        //Create new dataFrame with only `firstName` and `lastName` columns of persons from Chicago
        Dataset<Row> gangsters = personDF;

        //TODO
        //Create temporary view `gangsters` from gangsters dataFrame
        //hint: there is a method in dataFrame to do that
        gangsters.show();

        //TODO
        //Print all records from `gangsters` view ordered by lastName using select statement
        //hint: use `sql` method on `spark` object
        spark.sql("");
    }
}
