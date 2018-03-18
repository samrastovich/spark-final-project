import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.log4j.Logger
import org.apache.log4j.Level

//case class ttweet(Text : String, Date : String, Favorites : Int , Retweets : Int, TweetID : Int)


object TrumpAnalysis {

  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("helo worl")
    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    sc.textFile("datasets/trump/trump_tweets_small.csv")
      .map(x => x.split("\"")).foreach(x => println(x.toList.toString()))

    }
}

