
import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.NumberFormat
import java.io.File

case class Approval(startDate: String, pop: String, approval: Double, disapprove: Double)
case class Tweet(text: String, date: String, favorites: Int, retweets: Int)
object ApprovalAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val obamaApproval = readApproval("datasets/approval_ratings/obama.csv", sc)
    val obamaTweets = readTweets("datasets/obama/obama_tweets.csv", sc)

    val trumpApproval = readApproval("datasets/approval_ratings/trump.csv", sc)
    val trumpTweets = readTweets("datasets/trump/trump_tweets.csv", sc)

    val (approvals, tweets) = formatDates(obamaApproval, obamaTweets)
    val (tApprovals, tTweets) = formatDates(trumpApproval, trumpTweets)
    //compareObama(approvals, tweets)
    comparePopulation(approvals, tweets)
    //comparePopulation(tApprovals, tTweets)

  }

  def readApproval(file: String, sc: SparkContext): RDD[Approval] = {
    val reader = CSVReader.open(new File(file))

    val li = reader.allWithHeaders()
      .map(x => {
        Approval(x.getOrElse("start_date", "NULL"),
          x.getOrElse("survey_population", "NULL"),
          x.get("approve_percent") match {
            case None => 0.0
            case Some(c) => c match {
              case "" => 0.0
              case _ => c.toDouble
            }
          },
          x.get("disapprove_percent") match {
            case None => 0.0
            case Some(c) => c match {
              case "" => 0.0
              case _ => c.toDouble
            }
          })
      })
    sc.parallelize(li)
  }

  def readTweets(file: String, sc: SparkContext): RDD[Tweet] = {
    val reader = CSVReader.open(new File(file))

    val li = reader.allWithHeaders()
      .map(x => {
        Tweet(x.getOrElse("Text", "NULL"),
          x.getOrElse("Date", "NULL"),
          x.get("Favorites") match {
            case None => 0
            case Some(c) => c match {
              case "" => 0
              case _ => c.toInt
            }
          },
          x.get("Retweets") match {
            case None => 0
            case Some(c) => c match {
              case "" => 0
              case _ => c.toInt
            }
          })
      })
    sc.parallelize(li)
  }

  def formatDates(approvals: RDD[Approval], tweets: RDD[Tweet]): (RDD[Approval], RDD[Tweet]) = {
    val a = approvals.map(x => {
      val split = x.startDate.split(" ")(0).split("-")
      Approval(split(0),
        x.pop, x.approval, x.disapprove)
    })
    val t =  tweets.map(x => {
      val split = x.date.split(" ")(0).split("-")
      Tweet(x.text, split(0),
        x.favorites, x.retweets)
    })
    (a, t)
  }

  def compareObama(approvals: RDD[Approval], tweets: RDD[Tweet]): Unit = {
    val a = approvals
      .keyBy(_.startDate)
      .join(tweets.keyBy(_.date))
      .aggregateByKey((0, (0.0, 0.0)))(
        (accum, vals) => (accum._1 + 1, (accum._2._1 + vals._1.approval, accum._2._2 + vals._2.favorites)),
        (accum1, accum2) => (accum1._1 + accum2._1, (accum1._2._1 + accum2._2._1, accum1._2._2 + accum2._2._2))
      )
    val formatter = NumberFormat.getInstance()

    val fin = a.map(x => {
      (x._1, (formatter.format(x._2._2._1 / x._2._1), formatter.format(x._2._2._2 / x._2._1)))
    })

    fin.collect().foreach(println)
  }

  def comparePopulation(approvals: RDD[Approval], tweet: RDD[Tweet]): Unit = {
    val a = approvals
      .keyBy(x => (x.pop, x.startDate))
      .aggregateByKey((0, (0.0, 0.0)))(
        (accum, vals) => (accum._1 + 1, (accum._2._1 + vals.approval, accum._2._2 + vals.disapprove)),
        (accum1, accum2) => (accum1._1 + accum2._1, (accum1._2._1 + accum2._2._1, accum1._2._2 + accum2._2._2))
      )
    val formatter = NumberFormat.getInstance()

    val averageByPop = a.mapValues(x => {
      (formatter.format(x._2._1 / x._1), formatter.format(x._2._2 / x._1))
    })

    averageByPop.collect().foreach(println)
  }



}
