import org.apache.spark.rdd.RDD

package object myFunctions {

  case class Rating(user: Int, item: Int, rating: Double)

  // ************************************************************************************************************//

  // **************************************    Q3.1.1, Q3.1.2, Q3.1.3   *****************************************//

  // ************************************************************************************************************//

  def divide_tuple(u: (Int, Int)): Double = u._1.asInstanceOf[Double] / u._2.asInstanceOf[Double]

  // Define a function to compute the averageRatings for either a user or item using pattern matching
  def computeAverageRating(data: RDD[Rating], option: String): RDD[(Int, Double)] = option match {
    case "user" => data.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
    case "item" => data.map(x => (x.item, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
  }

  // Define the absolute error
  def absErr(x: Double, y: Double): Double = (x - y).abs

  // Define all close to mean method
  def allCloseToMean(x: RDD[Double], mean: Double, epsilon: Double): Boolean = {
    x.map(x => absErr(x, mean) < epsilon).reduce(_ && _) // Datatype of answer: Boolean
  }

  // Define Ratio Close to Mean
  def ratioCloseToMean(x: RDD[Double], mean: Double, epsilon: Double): Double = {
    divide_tuple(x.map(x => if (absErr(x, mean) < 0.5) 1 else 0).
      aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (u, v) => (u._1 + v._1, u._2 + v._2)))
  }


  // ************************************************************************************************************//

  // ******************************************    Q3.1.4, Q3.1.5   *********************************************//

  // ************************************************************************************************************//

  // Define a case class for our intermediate calculations and clarify the code
  case class ExtendedRating(user: Int, item: Int, pred: Double, groundTruthRating: Double)

  // Define a case class for Running time calculations
  case class RunningTime(min: Double, max: Double, avg: Double, std: Double)

  // Equation 3
  def scale(x: Double, v: Double) = if (x == v) 1 else if (x > v) 5 - v else v - 1

  // Equation 2
  def normalize(x: Double, v: Double) = (x - v) / scale(x, v)

  // Equation 5
  def predict(x: Double, y: Double): Double = x + y * scale((x + y), x)


  // Define a function to compute prediction (while storing the ground truth)
  def computePrediction(train: RDD[Rating], test: RDD[Rating], method: String): RDD[ExtendedRating] = method match {
    case "global" => {
      // Compute global average
      val globalPred = train.map(_.rating).mean()
      // Store the global prediction  in rating and the ground truth
      test.map(r => ExtendedRating(r.user, r.item, globalPred, r.rating))
    }
    case "user" => {
      // Compute global average
      val globalPred = train.map(_.rating).mean()
      // Compute global average per user
      val averageUserRatingsPerUser = computeAverageRating(train, "user")
      // Join test and averageUserRatingsPerUser on user. Store prediction and ground Truth
      test.map(r => (r.user, (r.item, r.rating))).leftOuterJoin(averageUserRatingsPerUser)
        .map(x => ExtendedRating(x._1, x._2._1._1, x._2._2.getOrElse(globalPred), x._2._1._2))
    }
    case "item" => {
      // Compute global average
      val globalPred = train.map(_.rating).mean()
      // Compute global average per item
      val averageUserRatingsPerItem = computeAverageRating(train, "item")
      // Join test and averageUserRatingsPerItem on item. Store prediction and ground Truth
      test.map(r => (r.item, (r.user, r.rating))).leftOuterJoin(averageUserRatingsPerItem)
        .map(x => ExtendedRating(x._2._1._1, x._1, x._2._2.getOrElse(globalPred), x._2._1._2))
    }
    // Compute Baseline Method
    case "baseline" => computeBaseline(train, test)
    // Q4.1.2
    case "adjustedBaseline" => computeAdjustedBaseline(train, test, 100)
  }

  def computeBaseline(train: RDD[Rating], test: RDD[Rating]): RDD[ExtendedRating] = {
    // Compute global average
    val globalPred = train.map(_.rating).mean()
    // Compute global average per user
    val averageUserRatingsPerUser = computeAverageRating(train, "user")

    // We extend replace the rating in the training data by the normalized deviation
    val extendedWithUserNormDev = train.map(x => (x.user, (x.item, x.rating))).
      join(averageUserRatingsPerUser).map(x => Rating(x._1, x._2._1._1, normalize(x._2._1._2, x._2._2)))

    // We Compute the globalAverageDeviation for each item
    val globalAverageDev = computeAverageRating(extendedWithUserNormDev, "item")

    // We join the testing data and the averageUserRatingsPerUser on the userID
    // We replace NaN values of the user by the global average
    val tmp = test.map(r => (r.user, (r.item, r.rating))).leftOuterJoin(averageUserRatingsPerUser)
      .map(x => ExtendedRating(x._1, x._2._1._1, x._2._2.getOrElse(globalPred), x._2._1._2))

    // We join the resulting RDD with the globalAverageDev on the itemID
    // We replace NaN values of globalAverageDev by 0 which corresponds to replacing by globalAvg
    // non rated items in the training set
    // Store prediction and ground Truth
    tmp.map(r => (r.item, (r.user, r.pred, r.groundTruthRating))).leftOuterJoin(globalAverageDev)
      .map(x => ExtendedRating(x._2._1._1, x._1, predict(x._2._1._2, x._2._2.getOrElse(0)), x._2._1._3))
  }

  // Define a function to compute the MAE
  def computeMae(train: RDD[Rating], test: RDD[Rating], method: String): Double = {
    // Compute prediction according to the method
    val prediction = computePrediction(train, test, method)
    // Compute MAE
    prediction.map(r => absErr(r.pred, r.groundTruthRating)).mean()
  }

  // Define computeTime function
  def computeTime(method: String, train: RDD[Rating], test: RDD[Rating]): RunningTime = {
    // Define an emptyList
    var elapsedTimes = List[Double]()
    // Iterate 10 times
    for (i <- 0 to 10) {
      val begin = System.nanoTime()
      computePrediction(train, test, method)
      // Compute elapsed timed dt
      val dt = ((System.nanoTime() - begin)) * 1E-3

      // Append to the list
      elapsedTimes = dt +: elapsedTimes
    }
    // Compute Mean and Std
    val mean = elapsedTimes.sum / elapsedTimes.size
    val std = scala.math.sqrt(elapsedTimes.map(x => scala.math.pow(x - mean, 2)).sum / elapsedTimes.size)

    // Store in RunningTime
    RunningTime(elapsedTimes.min, elapsedTimes.max, mean, std)
  }

  // ************************************************************************************************************//

  // ******************************************    Q4.1.1, Q4.1.2   *********************************************//

  // ************************************************************************************************************//

  // Define a case class to handle parsing data from our personal csv
  case class RatingParser(user: Int, item: Int, item_name: String, rating: Option[Double])

  // Defining functions to parse the csv File
  // We define a function to check if an element is a Double
  def toDouble(x: String): Option[Double] = {
    try {
      Some(x.toDouble)
    } catch {
      case e: Exception => None
    }
  }

  // Define the main parser function
  // The parsers handles line even with separator present in the movie title
  def convertToRatings(x: String, sep: Char): RatingParser = {
    // Split the string of each line using ','
    val list = x.split(sep)
    toDouble(list.last) match {
      // If the last element is a double and the last character is not ','
      case Some(u) => if (x.last != sep) {
        assert(u >= 0 && u <= 5, "Invalid rating")
        RatingParser(944, list.head.toInt, list.tail.dropRight(1).mkString(""), Some(u))
      } else
        RatingParser(944, list.head.toInt, list.tail.mkString(""), None)
      case None => RatingParser(944, list.head.toInt, list.tail.mkString(""), None)
    }
  }

  // Define function to compute Item Counts
  def computeItemCount(data: RDD[Rating]): RDD[(Int, Int)] = data.map(r => (r.item, 1)).reduceByKey(_ + _)

  // Define function to select most rated items (most rated in the sens of number of rates)
  def computeAvgMostRatedItemsCounts(itemCounts: RDD[(Int, Int)], top: Int): Double = {
    //println(itemCounts.sortBy(_._2, ascending = false).take(10).mkString("Array(", ", ", ")"))
    itemCounts.sortBy(_._2, ascending = false).values.take(top).sum/top.asInstanceOf[Double]
  }

  // Define our tanh function
  def scaledSigmoid(x: Double):Double = 1/(1+math.exp(-0.5*x))

  // Scale ratings according to the number of counts using tanh
  def scaleItemRating(rating: Double, itemCount: Int, mostRatedAvgCount: Double): Double =
    (rating-1)*scaledSigmoid(itemCount-mostRatedAvgCount)+1


  def computeAdjustedBaseline(train: RDD[Rating], test: RDD[Rating], top: Int): RDD[ExtendedRating] = {
    // Compute Item counts
    val itemCounts = computeItemCount(train)

    // Compute Average #top most rated counts
    val avgMostRatedCounts = computeAvgMostRatedItemsCounts(itemCounts, top)

    // Compute baseline prediction
    val baselinePrediction = computeBaseline(train, test)

    // Scale ratings according to the number of ratings
    baselinePrediction.map(r => (r.item, (r.user, r.pred, r.groundTruthRating))).join(itemCounts)
      .map(x => ExtendedRating(x._2._1._1, x._1, scaleItemRating(x._2._1._2, x._2._2, avgMostRatedCounts), x._2._1._3))
  }
}
