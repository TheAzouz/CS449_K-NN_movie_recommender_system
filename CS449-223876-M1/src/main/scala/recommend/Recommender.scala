package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

// importing all required functions
import myFunctions._

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


  // Parsing the personal.csv using our function
  val personalRDD = personalFile.map(x => convertToRatings(x,','))

  // Get Rated items and convert them to ratings
  val rated = personalRDD.filter(r => r.rating.isDefined).map(x => Rating(x.user,x.item,x.rating.get))

  // Get notRated items and convert them to ratings (we set the rating at 0)
  val notRated =  personalRDD.filter(r => r.rating.isEmpty).map(x => Rating(x.user,x.item,0))

  // Get item Names and collect them as Map
  val itemNames = personalRDD.map(r => (r.item,r.item_name)).collectAsMap()

  // Construct our training data by adding our ratings
  val train = data.union(rated)

  // Compute personal prediction for baseline Method
  val personalPredictionsBaseline = computePrediction(train,notRated,"baseline")
    .map(r => Rating(r.user,r.item,r.pred))

  // Get top5 results with the smallest item index
  val top5resultsBaseline = personalPredictionsBaseline.map( x => (x.item,x.rating))
    .sortBy(x => (x._2,-x._1),false).take(5)

  // Add movie names to our results
  val resultsBaseline = top5resultsBaseline.map( x=> List(x._1,itemNames.getOrElse(x._1,"Movie not found"),x._2))

  // Compute personal prediction for adjustedBaseline method
  val personalPredictionsAdjustedBaseline = computePrediction(train,notRated,"adjustedBaseline")
    .map(r => Rating(r.user,r.item,r.pred))

  // Get top5 results with the smallest item index
  val top5resultsAdjustedBaseline = personalPredictionsAdjustedBaseline
    .map( x => (x.item,x.rating)).sortBy(x => (x._2,-x._1),false).take(5)

  // Add movie names to our results
  val resultsAdjustedBaseline = top5resultsAdjustedBaseline
    .map( x=> List(x._1,itemNames.getOrElse(x._1,"Movie not found"),x._2))


  assert(personalFile.count == 1682, "Invalid personal data")


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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q4.1.1" -> List[Any](
            resultsBaseline(0), // Datatypes for answer: Int, String, Double
            resultsBaseline(1),
            resultsBaseline(2),
            resultsBaseline(3),
            resultsBaseline(4)
          ),
          "Q4.1.2" -> List[Any](
            resultsAdjustedBaseline(0), // Datatypes for answer: Int, String, Double
            resultsAdjustedBaseline(1),
            resultsAdjustedBaseline(2),
            resultsAdjustedBaseline(3),
            resultsAdjustedBaseline(4)
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