package training.day3.streaming

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import training.Utils._
import twitter4j.Status

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

    tweets.map(status => status.getText).print()

    //RDD of hashtags from the tweets RDD
    val hashtags = tweets.flatMap(_.getHashtagEntities).map(_.getText)

    //Count hashtags over last window
    val countedHashtags = hashtags.countByValue()

    //Sort by popularity
    val trendingHashtags = countedHashtags.transform(rdd => rdd.map(_.swap).sortByKey(ascending = false).values)

    //Print top 10 hashtags
    trendingHashtags.print()

    //(OPTIONAL) Print tweets with hashtags from top 10 trending hashtags
    val filterTrendingTweets: (RDD[Status], RDD[String]) => RDD[Status] = (tweetRDD, hashtagRDD) => {
      val topHashtags = hashtagRDD.take(10)
      tweetRDD.filter(tweet => {
        val hashtagEntities = tweet.getHashtagEntities
        hashtagEntities.exists(hashtag => topHashtags.contains(hashtag.getText))
      })
    }

    val trendingTweets = tweets.transformWith(trendingHashtags, filterTrendingTweets)
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