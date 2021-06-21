package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

//class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//  val train = opt[String](required = true)
//  val test = opt[String](required = true)
//  val json = opt[String]()
//  verify()
//}

import mileStone2Functions._

case class Rating(user: Int, item: Int, rating: Double)

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
//  println("Loading training data from: " + conf.train())
//  val trainFile = spark.sparkContext.textFile(conf.train())
//  val train = trainFile.map(l => {
//      val cols = l.split("\t").map(_.trim)
//      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
//  })
//  assert(train.count == 80000, "Invalid training data")
//
//  println("Loading test data from: " + conf.test())
//  val testFile = spark.sparkContext.textFile(conf.test())
//  val test = testFile.map(l => {
//      val cols = l.split("\t").map(_.trim)
//      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
//  })
//  assert(test.count == 20000, "Invalid test data")

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  val exercise = exercise3(conf,spark)

  //println(o.head.take(100).mkString("Array(", ", ", ")"))

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> exercise.listMae(10), // Datatype of answer: Double
            "MaeForK=30" -> exercise.listMae(30), // Datatype of answer: Double
            "MaeForK=50" -> exercise.listMae(50), // Datatype of answer: Double
            "MaeForK=100" -> exercise.listMae(100), // Datatype of answer: Double
            "MaeForK=200" -> exercise.listMae(200), // Datatype of answer: Double
            "MaeForK=300" -> exercise.listMae(300), // Datatype of answer: Double
            "MaeForK=400" -> exercise.listMae(400), // Datatype of answer: Double
            "MaeForK=800" -> exercise.listMae(800), // Datatype of answer: Double
            "MaeForK=943" -> exercise.listMae(943), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> exercise.lowestK, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> exercise.deltaBaseline // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> exercise.memoryUsage(10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> exercise.memoryUsage(30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> exercise.memoryUsage(50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> exercise.memoryUsage(100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> exercise.memoryUsage(200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> exercise.memoryUsage(300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> exercise.memoryUsage(400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> exercise.memoryUsage(800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> exercise.memoryUsage(943) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> exercise.availableRam, // Datatype of answer: Int
            "MaximumNumberOfUsersThatCanFitInRam" -> exercise.storableSim // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
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
