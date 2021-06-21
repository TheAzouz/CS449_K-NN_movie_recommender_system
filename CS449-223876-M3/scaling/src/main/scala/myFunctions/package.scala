import breeze.linalg._
import breeze.numerics._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

package object myFunctions {

  def scale(x: Double, v: Double): Double = if (x == v) 1 else if (x > v) 5 - v else v - 1
  def normalize(x: Double, v: Double): Double = (x - v) / scale(x, v)
  def predict(x: Double, y: Double): Double = x + y * scale(x + y, x)
  def round(x: Double, n: Int):Double = {
    val sig = math.pow(10,n)
    (x * sig).round/sig
  }


  def computeAverageRating(train:CSCMatrix[Double]):(Double,SparseVector[Double]) = {
    // Get matrix shape [n x m]
    val n_items = train.cols
    val ones = SparseVector[Double](Array.tabulate(n_items)(_ => 1.0))
    val activeElem = train.mapActiveValues(_ => 1.0)
    val sumUserRating = train * ones
    val numRatedItems = activeElem * ones

    (computeGlobalAverage(sumUserRating,numRatedItems),sumUserRating/numRatedItems)
  }

  def computeGlobalAverage(sumUserRating : SparseVector[Double],numRatedItems:SparseVector[Double]):Double = {
    val ones = SparseVector[Double](Array.tabulate(sumUserRating.size)(_ => 1.0))
    val totalSum = sumUserRating.t * ones
    val counts = numRatedItems.t * ones
    totalSum/counts
  }

  def computeNormalizedDeviation(train:CSCMatrix[Double], averageUserRatings : SparseVector[Double]):CSCMatrix[Double] = {

    val normalizedDeviation = new CSCMatrix.Builder[Double](rows = train.rows, cols = train.cols)

    for(((user,item),_) <- train.activeIterator) {
      normalizedDeviation.add(user,item,normalize(train(user,item),averageUserRatings(user)))
    }
    normalizedDeviation.result()
  }

  def computeNorm(ratingNormalizedDev:CSCMatrix[Double]): SparseVector[Double] = {
    val n_items = ratingNormalizedDev.cols
    val ones = SparseVector[Double](Array.tabulate(n_items)(_ => 1.0))
    val squaredMatrix = ratingNormalizedDev.mapActiveValues(x => math.pow(x,2))
    val sumOfSquares = squaredMatrix * ones
    sumOfSquares.mapActiveValues(x => math.sqrt(x))
  }

  // Define a function to preprocess ratings (Equation 4, Milestone 2)
  def preprocessRatings(ratingNormalizedDev:CSCMatrix[Double]):CSCMatrix[Double] = {
    //println("Preprocessing")
    // Compute the norm of the normalized rating with respect to each user
    val normUserNormalizedDev = computeNorm(ratingNormalizedDev)
    val preprocessed = new CSCMatrix.Builder[Double](rows = ratingNormalizedDev.rows, cols = ratingNormalizedDev.cols)
    for (((user,item),rating) <- ratingNormalizedDev.activeIterator) {
      preprocessed.add(user,item,rating/normUserNormalizedDev(user))
    }
    preprocessed.result()
  }


  def computeSimilarity(normalizedDeviation: CSCMatrix[Double],k:Int,sc:SparkContext):CSCMatrix[Double] = {

    //println("Computing Similarity")
    // Preprocess Ratings
    val ratings = preprocessRatings(normalizedDeviation)
    val brRatings = sc.broadcast(ratings)

    def topk(u:Int):SparseVector[Double] = {
      val ratings = brRatings.value
      val output = SparseVector.zeros[Double](ratings.rows)
      val slice = ratings(u,0 until ratings.cols).t.toDenseVector
      val intermOut = ratings * slice
      intermOut(u) = -1
      for (v <- argtopk(intermOut, k)) {
        output(v) = intermOut(v)
      }
      output
    }

    val kSvu = new CSCMatrix.Builder[Double](rows = ratings.rows, cols = ratings.rows)

    val sim = sc.parallelize( 0 until ratings.rows).map(topk).collect()
    for( u <- 0 until ratings.rows ){
      val knn = sim(u)
      for( (v,s) <- knn.activeIterator ){
        kSvu.add(v,u,s)
      }
    }

    kSvu.result()
  }


  def dot(u:Int,item:Int,Svu: CSCMatrix[Double], rating:CSCMatrix[Double]): Double = {
    Svu(0 until Svu.rows,u).toDenseVector.t * rating(0 until rating.rows,item).toDenseVector
  }

  def sparkPredict(kSuv: CSCMatrix[Double],train: CSCMatrix[Double], test: CSCMatrix[Double],sc:SparkContext,
                   brNormalizedDeviation: Broadcast[CSCMatrix[Double]],
                   brAverageUserRatings:Broadcast[SparseVector[Double]],
                  brGlobalAvg:Broadcast[Double]):CSCMatrix[Double] = {

    val brkSuv = sc.broadcast(kSuv)
    val brAbsKSuv = sc.broadcast(kSuv.mapActiveValues(math.abs))
    val brRatedItems = sc.broadcast(train.mapActiveValues(_ => 1.0))


    def predictUserItem(input:(Int,Int)):(Int,Int,Double) = {
      val u = input._1
      val item = input._2

      val denominator = dot(u,item,brAbsKSuv.value,brRatedItems.value)
      val nominator = dot(u,item,brkSuv.value,brNormalizedDeviation.value)
      val userDev = nominator / denominator
      val userAvg = brAverageUserRatings.value(u)

      (u,item,predict(if(userAvg == 0) brGlobalAvg.value else userAvg, if(userDev.isNaN) 0 else userDev))
    }

    val predictions = sc.parallelize(test.activeIterator.map{case ((u,i),gt) => (u,i)}.toList)
      .map(predictUserItem).collect()

    val predictionMx = new CSCMatrix.Builder[Double](rows = test.rows, cols = test.cols)

    for ((u,item,pred) <- predictions)
      {
        predictionMx.add(u,item,pred)
      }
    predictionMx.result()
  }


  //case class PredictionNN(predictions: Map[Int,RDD[ExtendedRating]], SuvNN: RDD[ExtendedSimilarity])
  case class Prediction(pred: CSCMatrix[Double], knnTime: Double, predTime: Double )

  def computePredictionForKnnWithSpark(train:CSCMatrix[Double],test:CSCMatrix[Double], sc: SparkContext, k :Int) = { //

    // Compute global average and users' average ratings
    val (globalAvg, averageUserRatings) = computeAverageRating(train)

    val brAverageUserRatings = sc.broadcast(averageUserRatings)

    val brGlobalAvg = sc.broadcast(globalAvg)

    // Compute Normalized Deviation
    val normalizedDeviation = computeNormalizedDeviation(train, averageUserRatings)

    val brNormalizedDeviation = sc.broadcast(normalizedDeviation)

    var begin = System.nanoTime()
    // Compute Suv
    val kSvu = computeSimilarity(normalizedDeviation,k,sc)

    val knnTime = (System.nanoTime()-begin) * 1e-3
    println(s"KnnTime = ${knnTime*1e-6}")

    begin = System.nanoTime()
    val prediction = sparkPredict(kSvu,train, test,sc:SparkContext, brNormalizedDeviation,
      brAverageUserRatings, brGlobalAvg)

    val predTime = (System.nanoTime()-begin) * 1e-3
    println(s"predTime = ${predTime*1e-6}")

    Prediction(pred = prediction,knnTime = knnTime, predTime = predTime)

  }

  def computeMae(prediction:CSCMatrix[Double],test:CSCMatrix[Double]):Double = {
    var counter = 0
    var accError = 0.0
    for((k,gt) <- test.activeIterator){
      accError += (gt - prediction(k)).abs
      counter += 1
    }
    round(accError/counter,4)
  }
  // Define a case class for Running time calculations
  case class Statistics(min: Double, max: Double, avg: Double, std: Double)

  // Define function to compute Min Max Mean Std of a List of doubles
  def computeStatistics(l: List[Double]):Statistics = {
    val mean = l.sum / l.size
    val std = scala.math.sqrt(l.map(x => scala.math.pow(x - mean, 2)).sum / l.size)
    Statistics(l.min, l.max, mean, std)
  }

  def question2(train:CSCMatrix[Double],test:CSCMatrix[Double], sc: SparkContext, k :Int) = {
    var elapsedTimesPred = List[Double]()
    var elapsedTimesKnn = List[Double]()
    for(i <- 1 to 5) {
      println(s"*** Iteration = $i ***")
      val tmp = computePredictionForKnnWithSpark(train,test,sc,k)
      elapsedTimesPred = tmp.predTime +: elapsedTimesPred
      elapsedTimesKnn = tmp.knnTime +: elapsedTimesKnn
    }
    val knnStat = computeStatistics(elapsedTimesKnn)
    val predStat = computeStatistics(elapsedTimesPred)
    println("knnStat",knnStat)
    println("predStat",predStat)
  }

  def question3(train:CSCMatrix[Double],test:CSCMatrix[Double], k :Int, sc: SparkContext) = {

    var elapsedTimesPred = List[Double]()
    var elapsedTimesKnn = List[Double]()

    for (i <- 1 to 3){
      println(s"*** Iteration = $i ***")
      val tmp = computePredictionForKnnWithSpark(train,test,sc,k)
      elapsedTimesPred = tmp.predTime +: elapsedTimesPred
      elapsedTimesKnn = tmp.knnTime +: elapsedTimesKnn
    }
    val knnStat = computeStatistics(elapsedTimesKnn)
    val predStat = computeStatistics(elapsedTimesPred)
    println("knnStat",knnStat)
    println("predStat",predStat)

  }

}