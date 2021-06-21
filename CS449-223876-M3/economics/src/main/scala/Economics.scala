import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }

    def round(x: Double):Double = {
      val sig = math.pow(10,4)
      (x * sig).round/sig
    }

    val icClusterCost = 35000.0
    val icClusterCostPerDay = 20.40
    val icClusterRam = 1.0 * 24 * 64
    val icClusterNumCpu = 2 * 14

    val containerPricePerGRam = 0.012
    val containerPricePerCpuCore = 0.088

    val containerCostPerDayEqICC = icClusterRam*containerPricePerGRam + containerPricePerCpuCore*icClusterNumCpu

    val containerCostPerDayEqRPi = 4 * 8 * containerPricePerGRam + containerPricePerCpuCore

    val maxRPiCostPerDay = 0.054
    val minRPiCostPerDay = 0.0108

    val Ratio4RPi_over_Container_MaxPower = round(4*maxRPiCostPerDay/containerCostPerDayEqRPi)
    val Ratio4RPi_over_Container_MinPower = round(4*minRPiCostPerDay/containerCostPerDayEqRPi)
    val cost4RPI = 4 * 94.83
    val costRPI = 94.83

    val nbrRPi = math.floor(icClusterCost/costRPI)

    val memoryPerUser = (1.0 * 100 + 4.0 * 200) * 1E-9



    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> math.ceil(icClusterCost/icClusterCostPerDay),  // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" ->round( (icClusterCost/icClusterCostPerDay) / 365) // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> containerCostPerDayEqICC, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> round(icClusterCostPerDay/containerCostPerDayEqICC), // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> (containerCostPerDayEqICC<icClusterCostPerDay) // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> containerCostPerDayEqRPi, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> Ratio4RPi_over_Container_MaxPower, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> Ratio4RPi_over_Container_MinPower, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> (4*maxRPiCostPerDay/containerCostPerDayEqICC > 1.0/.95) // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> math.ceil(cost4RPI/(containerCostPerDayEqRPi-4*minRPiCostPerDay)), // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> math.ceil(cost4RPI/(containerCostPerDayEqRPi-4*maxRPiCostPerDay)) // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> nbrRPi, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> round(nbrRPi/4/(14*2)), // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> round(nbrRPi*8/(64*24)) // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> math.floor(0.5 * 1/ memoryPerUser), // Datatype of answer: Double
              "NbUserPerRPi" -> math.floor(0.5 * 8 / memoryPerUser), // Datatype of answer: Double
              "NbUserPerICC.M7" -> math.floor(0.5 * 64 * 24/ memoryPerUser) // Datatype of answer: Double
            )
            // Answer the Question 5.1.7 exclusively on the report.
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
