package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import mileStone2Functions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

//case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  assert(personalFile.count == 1682, "Invalid personal data")


  val recommendation = recommend(data,personalFile)


  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" ->
              List[Any](
                recommendation(30)(0), // Datatypes for answer: Int, String, Double
                recommendation(30)(1), // Representing: Movie Id, Movie Title, Predicted Rating
                recommendation(30)(2), // respectively
                recommendation(30)(3),
                recommendation(30)(4),
              ),

            "Top5WithK=300" ->
              List[Any](
                recommendation(300)(0), // Datatypes for answer: Int, String, Double
                recommendation(300)(1), // Representing: Movie Id, Movie Title, Predicted Rating
                recommendation(300)(2), // respectively
                recommendation(300)(3),
                recommendation(300)(4),
              )

            // Discuss the differences in rating depending on value of k in the report.
          )
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
