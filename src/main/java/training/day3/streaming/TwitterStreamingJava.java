package training.day3.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.util.ArrayDeque;

import static training.Utils.*;

public class TwitterStreamingJava {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Twitter streaming java")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(30));

        setupOAuth();
        JavaDStream<Status> tweets = TwitterUtils.createStream(jssc);

        //Print tweets
        tweets.map(status -> status.getText()).print();

        //TODO
        //DStream of hashtags from the tweets DStream
        //Write an implementation based on `tweets` dstream instead of `jssc.queueStream(new ArrayDeque<>());`
        JavaDStream<String> hashtags = jssc.queueStream(new ArrayDeque<>());

        //TODO
        //Count hashtags over last window
        //Using `hashtags` write correct implementation for counting hashtags
        JavaPairDStream<String, Long> countedHashtags = jssc.queueStream(new ArrayDeque<>()).mapToPair(x -> new Tuple2<>("", 0L));

        //TODO
        //Sort by popularity
        //Using `countedHashtags` get new dstream with hashtags
        //ordered by popularity where most popular hashtag are at the top
        JavaDStream<String> trendingHashtags = jssc.queueStream(new ArrayDeque<>());

        //Print top 10 hashtags
        trendingHashtags.print();

        //TODO
        //(OPTIONAL) Print tweets with hashtags from top 10 trending hashtags
        //Using `transformWith` transformation on `tweets` dstream create new dstream of tweets
        //new dstream should contain only those tweets that have hashtags from the top 10 hashtags of `trendingHashtags`
        JavaDStream<Status> trendingTweets = jssc.queueStream(new ArrayDeque<>());

        trendingTweets.map(status -> formatTweet(status)).print();

        jssc.start();
        jssc.awaitTermination();
    }

    private static String formatTweet(Status tweet) {
        return tweet.getText() + "\n===========================================";
    }

    private static void setupOAuth() throws IOException {
        System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY);
        System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET);
        System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
        System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);
    }
}
