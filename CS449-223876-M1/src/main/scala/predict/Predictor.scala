package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

// importing all required functions
import myFunctions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

//case class Rating(user: Int, item: Int, rating: Double)



object Predictor extends App {
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
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

  // All the required functions are located in myFunctions package

  val globalTime = computeTime("global",train,test)
  val userTime = computeTime("user",train,test)
  val itemTime = computeTime("item",train,test)
  val baselineTime = computeTime("baseline",train,test)

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
          "Q3.1.4" -> Map(
            "MaeGlobalMethod" -> computeMae(train, test, "global"), // Datatype of answer: Double
            "MaePerUserMethod" -> computeMae(train, test, "user"), // Datatype of answer: Double
            "MaePerItemMethod" -> computeMae(train, test, "item"), // Datatype of answer: Double
            "MaeBaselineMethod" -> computeMae(train, test, "baseline") // Datatype of answer: Double
          ),

          "Q3.1.5" -> Map(
            "DurationInMicrosecForGlobalMethod" -> Map(
              "min" -> globalTime.min, // Datatype of answer: Double
              "max" -> globalTime.max, // Datatype of answer: Double
              "average" -> globalTime.avg, // Datatype of answer: Double
              "stddev" -> globalTime.std // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerUserMethod" -> Map(
              "min" -> userTime.min, // Datatype of answer: Double
              "max" -> userTime.max, // Datatype of answer: Double
              "average" -> userTime.avg, // Datatype of answer: Double
              "stddev" -> userTime.std // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerItemMethod" -> Map(
              "min" -> itemTime.min, // Datatype of answer: Double
              "max" -> itemTime.max, // Datatype of answer: Double
              "average" -> itemTime.avg, // Datatype of answer: Double
              "stddev" -> itemTime.std // Datatype of answer: Double
            ),
            "DurationInMicrosecForBaselineMethod" -> Map(
              "min" -> baselineTime.min, // Datatype of answer: Double
              "max" -> baselineTime.max, // Datatype of answer: Double
              "average" -> baselineTime.avg, // Datatype of answer: Double
              "stddev" -> baselineTime.std // Datatype of answer: Double
            ),
            "RatioBetweenBaselineMethodAndGlobalMethod" -> baselineTime.avg/globalTime.avg // Datatype of answer: Double
          ),
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