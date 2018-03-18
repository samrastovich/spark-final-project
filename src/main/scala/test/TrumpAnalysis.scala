package test
import java.io.File

import com.github.tototoshi.csv._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TrumpAnalysis {

  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    println("Trump Semantic Analysis")

    val TrumpTweets = "datasets/trump/trump_tweets.csv"
    val positiveWords = sc.textFile("datasets/opinion-words/positive.txt")
    val negativeWords = sc.textFile("datasets/opinion-words/negative.txt")
    val reader = CSVReader.open(new File(TrumpTweets))

    case class ttweet(text : String, date : String, favorites : String , retweets : String, tweetID : String)
    case class tweetOpinion(tweetId : String, favorites : String , retweets : String, positive : Long, negative : Long)

    val tweets = reader.allWithHeaders()
      .map(x=>ttweet(x.getOrElse("Text", "NULL"),
        x.getOrElse("Date", "NULL"),
        x.getOrElse("Favorites", "NULL"),
        x.getOrElse("Retweets", "NULL"),
        x.getOrElse("Tweet ID", "NULL")))

    tweets.map(x => {
      val twee = sc.parallelize(x.text.split(" ")).map(x => x.toLowerCase())
      val negWords = negativeWords.map(x => x.toLowerCase)
      val posWords = positiveWords.map(x => x.toLowerCase)
      tweetOpinion(x.tweetID, x.favorites, x.retweets, findNegative(twee , negWords), findPositive(twee, posWords))})
    }.foreach(println)

  def findNegative(tweet : RDD[String], negativeWords : RDD[String]) : Long = {
    tweet.intersection(negativeWords).count()
  }
  def findPositive(tweet : RDD[String], positiveWords : RDD[String]) : Long = {
    tweet.intersection(positiveWords).count()
  }
}


