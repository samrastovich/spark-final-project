package test
import java.io.File
import scala.io.Source
import com.github.tototoshi.csv._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ObamaAnalysis {

  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    println("Obama Semantic Analysis")

    val positiveWords = Source.fromFile("datasets/opinion-words/positive.txt").getLines().toList
    val negativeWords = Source.fromFile("datasets/opinion-words/negative.txt").getLines().toList
    val reader = CSVReader.open(new File("datasets/obama/obama_tweets.csv"))

    case class ttweet(text : String, date : String, favorites : String , retweets : String, tweetID : String)
    case class tweetOpinion(tweetId : String, favorites : String , retweets : String, positive : Long, negative : Long)

    val tweets = sc.parallelize(reader.allWithHeaders()
      .map(x=>ttweet(x.getOrElse("Text", "NULL"),
        x.getOrElse("Date", "NULL"),
        x.getOrElse("Favorites", "NULL"),
        x.getOrElse("Retweets", "NULL"),
        x.getOrElse("Tweet ID", "NULL"))))

    tweets.map(x => {
      val twee = x.text.split(" ").map(x => x.toLowerCase()).toList
      val posNeg = findNegAndPos(twee, negativeWords, positiveWords)
      tweetOpinion(x.tweetID, x.favorites, x.retweets, posNeg._1, posNeg._2)})
      .map(x => {
        if(x.positive > x.negative)
          (1, (x.favorites.toInt, x.retweets.toInt))
        else if (x.positive < x.negative)
          (-1, (x.favorites.toInt, x.retweets.toInt))
        else
          (0, (x.favorites.toInt, x.retweets.toInt))})
      .aggregateByKey((0, (0.0, 0.0)))(
        (counta, vals) => (counta._1 + 1, (counta._2._1 + vals._1, counta._2._2 + vals._2 )),
        (accum1, accum2) => (accum1._1 + accum2._1, (accum1._2._1 + accum2._2._1, accum1._2._2 + accum2._2._2 )))
      .map(x => (x._1, x._2._2._1/x._2._1, x._2._2._1, x._2._2._2/x._2._1, x._2._2._2,  x._2._1))
      .foreach(x => {
        val afav = BigDecimal(x._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val aret = BigDecimal(x._4).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        if(x._1 < 0)
          println("Total Negative Tweets: "+ x._6 + "\n[Average Favorites: " + afav + " Total Favorites: " + x._3 + " Average Retweets: " + aret + " Total Retweets: " + x._5+ "]\n")
        else if (x._1 > 0)
          println("Total Positive Tweets: "+ x._6 + "\n[Average Favorites: " + afav + " Total Favorites: " + x._3 + " Average Retweets: " + aret + " Total Retweets: " + x._5+ "]\n")
        else
          println("Total Neutral Tweets: "+ x._6 + "\n[Average Favorites: " + afav + " Total Favorites: " + x._3 + " Average Retweets: " + aret + " Total Retweets: " + x._5 + "]\n")
      })
  }

  def findNegAndPos(tweet : List[String], negativeWords : List[String], positiveWords : List[String]) : (Int,Int) = {
    (tweet.intersect(negativeWords).length,tweet.intersect(positiveWords).length)
  }
}