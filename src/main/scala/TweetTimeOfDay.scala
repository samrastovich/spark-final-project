import java.io.File

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TweetTimeOfDay {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val tweets_1 = "datasets/obama/obama_tweets.csv"
    val tweets_2 = "datasets/trump/trump_tweets.csv"
    var reader = CSVReader.open(new File(tweets_1))

    println("---- OBAMA TWEETS ----")
    val tweetsObama = sc.parallelize(reader.allWithHeaders()
      .map(x=>(x("Date").split(" ")(1).substring(0, 2),1)))
      .groupByKey()
      .map(x=>(x._1, x._2.size))
      .sortByKey()
      .collect()
      .foreach(println)
    reader.close()

    reader = CSVReader.open(new File(tweets_2))
    println("---- TRUMP TWEETS ----")
    val tweetsTrump = sc.parallelize(reader.allWithHeaders()
      .map(x=>(x("Date").split(" ")(1).substring(0, 2),1)))
      .groupByKey()
      .map(x=>(x._1, x._2.size))
      .sortByKey()
      .collect()
      .foreach(println)
    reader.close()

  }
}
