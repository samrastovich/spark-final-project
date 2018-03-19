
import java.io.File
import java.text.NumberFormat

import com.github.tototoshi.csv.CSVReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ObamaApprovalAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val tweets_1 = "datasets/obama/obama_tweets.csv"
    val tweets_2 = "datasets/trump/trump_tweets.csv"
    val apprObama = "datasets/approval_ratings/obama.csv"
    val apprTrump = "datasets/approval_ratings/trump.csv"

    //val readerObama = CSVReader.open(new File(tweets_1));
    val approvalReader = CSVReader.open(new File(apprObama))

    val numberFormat = NumberFormat.getInstance()

    case class Approval(approval: Double, disapproval: Double, organization: String, affiliation: String, date: String)

    val approvals = sc.parallelize(approvalReader.allWithHeaders()
      .map(x=>Approval( x.get("approve_percent") match {
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
        },
        x("survey_organization"),
        x("party_affiliation"),
        x("end_date").substring(0,4))))
      .persist()

      println("avg approval ratings per year for democratic organizations")
      approvals
      .filter{x => x.affiliation == "Democrat"}
      .keyBy(x=>x.date)
      .aggregateByKey((0, (0.0, 0.0)))(
        (x,y) => (x._1 + 1, (x._2._1 + y.approval, x._2._2 + y.disapproval)),
        (x2, y2) => (x2._1 + y2._1, (x2._2._1 + y2._2._1, x2._2._2 + y2._2._2))
      )
      .map(x=> (x._1, (numberFormat.format(x._2._2._1/x._2._1), numberFormat.format(x._2._2._2/x._2._1))))
      .sortByKey()
      .collect()
      .foreach(println)

    println("avg approval ratings per year for republican organizations")
    approvals
      .filter{x => x.affiliation == "Republican"}
      .keyBy(x=>x.date)
      .aggregateByKey((0, (0.0, 0.0)))(
        (x,y) => (x._1 + 1, (x._2._1 + y.approval, x._2._2 + y.disapproval)),
        (x2, y2) => (x2._1 + y2._1, (x2._2._1 + y2._2._1, x2._2._2 + y2._2._2))
      )
      .map(x=> (x._1, (numberFormat.format(x._2._2._1/x._2._1), numberFormat.format(x._2._2._2/x._2._1))))
      .sortByKey()
      .collect()
      .foreach(println)

    println("avg approval ratings per year for organizations with no party affiliations")
    approvals
      .filter{x => x.affiliation == "None"}
      .keyBy(x=>x.date)
      .aggregateByKey((0, (0.0, 0.0)))(
        (x,y) => (x._1 + 1, (x._2._1 + y.approval, x._2._2 + y.disapproval)),
        (x2, y2) => (x2._1 + y2._1, (x2._2._1 + y2._2._1, x2._2._2 + y2._2._2))
      )
      .map(x=> (x._1, (numberFormat.format(x._2._2._1/x._2._1), numberFormat.format(x._2._2._2/x._2._1))))
      .sortByKey()
      .collect()
      .foreach(println)
//      .aggregateByKey((0, (0.0, 0.0)))(
//        (accum, vals) => (accum._1 + 1, (accum._2._1 + vals._1.approval, accum._2._2 + vals._2.favorites)),
//        (accum1, accum2) => (accum1._1 + accum2._1, (accum1._2._1 + accum2._2._1, accum1._2._2 + accum2._2._2))
//      )


//    case class Tweet(msg: String, date: String, favs: String, retweets: String)
//
//    println("---- OBAMA TWEETS ----")
//    val tweetsObama = sc.parallelize(readerObama.allWithHeaders()
//      .map(x=>Tweet(x("Text"),
//        x("Date").split(" ")(0),
//        x("Favorites"),
//        x("Retweets"))))
//      .sortBy(x=>x.favs.toInt, false)
//      .collect()
//      .take(10)
//      .foreach(println)
//    readerObama.close()
//    println("---- TRUMP TWEETS ----")
//    val readerTrump = CSVReader.open(new File(tweets_2));
//    val tweetsTrump = sc.parallelize(readerTrump.allWithHeaders()
//      .map(x=>Tweet(x("Text"),
//        x("Date").split(" ")(0),
//        x("Favorites"),
//        x("Retweets"))))
//      .sortBy(x=>x.favs.toInt, false)
//      .collect()
//      .take(10)
//      .foreach(println)
//    readerTrump.close()



  }
}
