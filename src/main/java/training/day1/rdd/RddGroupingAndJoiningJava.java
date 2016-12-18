package training.day1.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

import static training.Utils.DATA_DIRECTORY_PATH;

public class RddGroupingAndJoiningJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RDD grouping and joining java")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Person> personRdd = sparkContext
                .textFile(DATA_DIRECTORY_PATH + "persons.csv")
                .map(line -> {
                    String[] columns = line.split(",");
                    return new Person(columns[0], columns[1], columns[2]);
                });

        JavaPairRDD<String, String> zipcodeRDD = sparkContext
                .textFile(DATA_DIRECTORY_PATH + "zip.csv")
                .mapToPair(line -> {
                    String[] columns = line.split(",");
                    return new Tuple2<>(columns[1], columns[0]);
                });

        Map<String, Iterable<Person>> groupedByCity = personRdd.groupBy(person -> person.city).collectAsMap();
        for (Map.Entry<String, Iterable<Person>> entry : groupedByCity.entrySet()) {
            System.out.println(entry.getKey() + " persons: " + entry.getValue());
        }

        JavaPairRDD<String, Person> personPairRDD = personRdd.keyBy(Person::getCity);

        JavaPairRDD<String, Tuple2<Person, String>> joined = personPairRDD.join(zipcodeRDD);
        Map<Person, String> personToZipCode = joined.values().mapToPair(tuple -> tuple).collectAsMap();
        for (Map.Entry<Person, String> entry : personToZipCode.entrySet()) {
            System.out.println(entry.getKey() + " zip code: " + entry.getValue());
        }
    }

    static class Person implements Serializable {
        private String firstName;
        private String lastName;
        private String city;

        public Person(String firstName, String lastName, String city) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.city = city;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public String getCity() {
            return city;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }
}

