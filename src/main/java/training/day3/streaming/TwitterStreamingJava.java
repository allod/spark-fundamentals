package training.day3.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

        tweets.map(status -> status.getText()).print();

        //RDD of hashtags from the tweets RDD
        JavaDStream<String> hashtags = tweets.flatMap(tweet -> Arrays.asList(tweet.getHashtagEntities()).iterator())
                .map(hashtagEntity -> hashtagEntity.getText());

        //Count hashtags over last window
        JavaPairDStream<String, Long> countedHashtags = hashtags.countByValue();

        //Sort by popularity
        JavaDStream<String> trendingHashtags = countedHashtags.transform(rdd -> rdd
                .mapToPair(tuple -> tuple.swap())
                .sortByKey(false)
                .values());

        //Print top 10 hashtags
        trendingHashtags.print();

        //(OPTIONAL) Print tweets with hashtags from top 10 trending hashtags
        JavaDStream<Status> trendingTweets = tweets.transformWith(trendingHashtags, (tweetRDD, hashtagRDD, time) -> {
            List<String> topHashtags = hashtagRDD.take(10);

            return tweetRDD.filter(tweet -> {
                List<HashtagEntity> hashtagEntities = Arrays.asList(tweet.getHashtagEntities());
                return hashtagEntities.stream().anyMatch(hashtag -> topHashtags.contains(hashtag.getText()));
            });
        });

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
