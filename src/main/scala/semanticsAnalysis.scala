
import java.io.File

import com.github.tototoshi.csv.CSVReader

object semanticsAnalysis {
  def main(args: Array[String]): Unit = {
    println("Semantics Analysis")

    val tweets_1 = "datasets/obama/obama_tweets.csv"
    val tweets_2 = "datasets/obama/whtweets.csv"
    val tweets_3 = "datasets/obama/BarackObama.csv"

    val approvalRatings_Obama = "datasets/approval_ratings/obama.csv"
    val reader = CSVReader.open(new File(approvalRatings_Obama));

    case class Rating(approval: String, disapproval: String, date: String, sample: String, surveyOrg: String)
    val tweets = reader.allWithHeaders()
      .map(x=>Rating(x.getOrElse("approve_percent", "NULL"),
        x.getOrElse("disapprove_percent", "NULL"),
        x.getOrElse("end_date", "NULL"),
        x.getOrElse("survey_sample", "NULL"),
        x.getOrElse("survey_organization", "NULL")))
      .foreach(x=>println(x))




  }
}
