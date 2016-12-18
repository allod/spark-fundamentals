package training.day3.streaming

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import training.Utils._
import twitter4j.Status

import scala.collection.mutable

object TwitterStreamingScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Twitter streaming scala")
      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(30))

    setupOAuth()
    val tweets = TwitterUtils.createStream(streamingContext, None)

    //Print tweets
    tweets.map(status => status.getText).print()

    //TODO
    //DStream of hashtags from the tweets DStream
    //Write an implementation based on `tweets` dstream instead of `streamingContext.queueStream(new mutable.Queue[RDD[String]])`
    val hashtags = streamingContext.queueStream(new mutable.Queue[RDD[String]])

    //TODO
    //Count hashtags over last window
    //Using `hashtags` write correct implementation for counting hashtags
    val countedHashtags = streamingContext.queueStream(new mutable.Queue[RDD[String]]).map(x => ("", 0L))

    //TODO
    //Sort by popularity
    //Using `countedHashtags` get new dstream with hashtags
    //ordered by popularity where most popular hashtag are at the top
    val trendingHashtags = streamingContext.queueStream(new mutable.Queue[RDD[String]])

    //Print top 10 hashtags
    trendingHashtags.print()

    //TODO
    //(OPTIONAL) Print tweets with hashtags from top 10 trending hashtags
    //Using `transformWith` transformation on `tweets` dstream create new dstream of tweets
    //new dstream should contain only those tweets that have hashtags from the top 10 hashtags of `trendingHashtags`

    val trendingTweets = streamingContext.queueStream(new mutable.Queue[RDD[Status]])
    trendingTweets.map(status => formatTweet(status)).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def formatTweet(tweet: Status): String = s"${tweet.getText}\n==========================================="

    def setupOAuth(): Unit = {
    val props = new Properties()
    val path = getClass.getResource("/configuration.properties").getPath
    props.load(new FileInputStream(path))

    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)
  }
}