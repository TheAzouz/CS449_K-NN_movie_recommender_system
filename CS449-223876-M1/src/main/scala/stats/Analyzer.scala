package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import myFunctions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}


object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(data.count == 100000, "Invalid data")

  val globalAverageRating = data.map(_.rating).mean()

  val averageUserRatingsPerUser = computeAverageRating(data, "user").values

  val averageUserRatingsPerItem = computeAverageRating(data, "item").values


  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach {
      f =>
        try {
          f.write(content)
        } finally {
          f.close
        }
    }

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> globalAverageRating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
              // Using as your input data the average rating for each user,
              // report the min, max and average of the input data.
              "min" -> averageUserRatingsPerUser.min, // Datatype of answer: Double
              "max" -> averageUserRatingsPerUser.max, // Datatype of answer: Double
              "average" -> averageUserRatingsPerUser.mean // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> allCloseToMean(averageUserRatingsPerUser,globalAverageRating, 0.5), // Datatype of answer: Boolean
        "RatioUsersCloseToGlobalAverageRating" -> ratioCloseToMean(averageUserRatingsPerUser,globalAverageRating, 0.5)

        ),
        "Q3.1.3" -> Map(
          "ItemsAverageRating" -> Map(
            // Using as your input data the average rating for each item,
            // report the min, max and average of the input data.
            "min" -> averageUserRatingsPerItem.min, // Datatype of answer: Double
            "max" -> averageUserRatingsPerItem.max, // Datatype of answer: Double
            "average" -> averageUserRatingsPerItem.mean // Datatype of answer: Double
          ),
          "AllItemsCloseToGlobalAverageRating" -> allCloseToMean(averageUserRatingsPerItem,globalAverageRating,0.5), // Datatype of answer: Boolean
          "RatioItemsCloseToGlobalAverageRating" -> ratioCloseToMean(averageUserRatingsPerItem,globalAverageRating,0.5) // Datatype of answer: Double
        )
        ,
        )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
