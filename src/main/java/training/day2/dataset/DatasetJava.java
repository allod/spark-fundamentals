package training.day2.dataset;

import org.apache.spark.sql.*;

import static training.Utils.DATA_DIRECTORY_PATH;

public class DatasetJava {
    public static void main(String[] args) {
        String zipTablePath = DATA_DIRECTORY_PATH + "zip.csv";
        String personTablePath = DATA_DIRECTORY_PATH + "persons.parquet";

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Dataset examples java")
                .getOrCreate();

        Encoder<ZipCode> zipCodeEncoder = Encoders.bean(ZipCode.class);
        Dataset<ZipCode> zipCodeDS = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(zipTablePath)
                .as(zipCodeEncoder);

        //Print zipCode schema
        zipCodeDS.printSchema();

        //Print first 20 rows from zipCode dataset
        zipCodeDS.show();

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDS = spark.read()
                .parquet(personTablePath)
                .as(personEncoder);

        //Print person schema
        personDS.printSchema();

        //Print first 20 rows from person dataset
        personDS.show();

        Column joinCondition = personDS.col("zip").equalTo(zipCodeDS.col("zip"));
        Dataset<Row> joined = personDS.join(zipCodeDS, joinCondition);
        Dataset<Row> personInfo = joined.select("firstName", "lastName", "city");

        //Print first 20 rows from joined dataset
        personInfo.show();

        //TODO
        //Print distinct three word counties from zipCode dataset
        zipCodeDS.show();

        //TODO
        //Print most popular names in person dataset
        personDS.show();

        //TODO
        //Print number of people by state
        zipCodeDS.join(personDS, "zip");

        //TODO
        //Save to json file cities that have more then five companies
        zipCodeDS.join(personDS, "zip");
    }

    static class Person {
        private String firstName;
        private String lastName;
        private String companyName;
        private int zip;
        private String email;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getCompanyName() {
            return companyName;
        }

        public void setCompanyName(String companyName) {
            this.companyName = companyName;
        }

        public int getZip() {
            return zip;
        }

        public void setZip(int zip) {
            this.zip = zip;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }

    static class ZipCode {
        private int zip;
        private String city;
        private String county;
        private String state;

        public int getZip() {
            return zip;
        }

        public void setZip(int zip) {
            this.zip = zip;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getCounty() {
            return county;
        }

        public void setCounty(String county) {
            this.county = county;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }
}
