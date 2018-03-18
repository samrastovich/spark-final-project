
import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

case class Approval(startDate: String, pop: String, approval: String, disapprove: String)
case class Tweet(text: String, date: String, favorites: String, retweets: String)
object ApprovalAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val obamaApproval = readApproval("datasets/approval_ratings/obama.csv", sc)
    val obamaTweets = readTweets("datasets/obama/obama_tweets.csv", sc)
    val (approvals, tweets) = formatDates(obamaApproval, obamaTweets)
    tweets.collect().foreach(println)

  }

  def readApproval(file: String, sc: SparkContext): RDD[Approval] = {
    val reader = CSVReader.open(new File(file))

    val li = reader.allWithHeaders()
      .map(x => {
        Approval(x.getOrElse("start_date", "NULL"),
          x.getOrElse("survey_population", "NULL"),
          x.getOrElse("approve_percent", "NULL"),
          x.getOrElse("disapprove_percent", "NULL"))
      })
    sc.parallelize(li)
  }

  def readTweets(file: String, sc: SparkContext): RDD[Tweet] = {
    val reader = CSVReader.open(new File(file))

    val li = reader.allWithHeaders()
      .map(x => {
        Tweet(x.getOrElse("Text", "NULL"),
          x.getOrElse("Date", "NULL"),
          x.getOrElse("Favorites", "NULL"),
          x.getOrElse("Retweets", "NULL"))
      })
    sc.parallelize(li)
  }

  def formatDates(approvals: RDD[Approval], tweets: RDD[Tweet]): (RDD[Approval], RDD[Tweet]) = {
    val a = approvals.map(x => {
      val split = x.startDate.split(" ")(0).split("-")
      Approval(split(0) + "-" + split(1),
        x.pop, x.approval, x.disapprove)
    })
    val t =  tweets.map(x => {
      val split = x.date.split(" ")(0).split("-")
      Tweet(x.text, split(0) + "-" + split(1),
        x.favorites, x.retweets)
    })
    (a, t)
  }

  def compare(approvals: RDD[Approval], tweets: RDD[Tweet]): Unit = {
    val a = approvals
      .keyBy(_.startDate)

  }

}
