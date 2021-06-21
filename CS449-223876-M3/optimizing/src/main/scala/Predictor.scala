import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import myFunctions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    println("Compute kNN on train data...")
    
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    val  exercise = Exercise(train,test)

    println("Compute predictions on test data...")

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
            "Q3.3.1" -> Map(
              "MaeForK=100" -> computeMae(exercise.prediction(100).pred,test), // Datatype of answer: Double
              "MaeForK=200" -> computeMae(exercise.prediction(200).pred,test)  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> exercise.knnStat.min,  // Datatype of answer: Double
                "max" -> exercise.knnStat.max, // Datatype of answer: Double
                "average" -> exercise.knnStat.avg, // Datatype of answer: Double
                "stddev" -> exercise.knnStat.std // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> exercise.predStat.min,  // Datatype of answer: Double
                "max" -> exercise.predStat.max, // Datatype of answer: Double
                "average" -> exercise.predStat.avg, // Datatype of answer: Double
                "stddev" -> exercise.predStat.std // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
