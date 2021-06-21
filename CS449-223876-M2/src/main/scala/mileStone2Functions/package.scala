import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._



package object mileStone2Functions {
  case class Rating(user: Int, item: Int, rating: Double)
  // Define a case class for our intermediate calculations and clarify the code
  case class ExtendedRating(user: Int, item: Int, pred: Double, groundTruthRating: Double)


  // Define a case class to store similarities
  case class Similarity(u: Int, v: Int, similarity: Double,count: Int)

  // Define Configuration File
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val train = opt[String](required = true)
    val test = opt[String](required = true)
    val json = opt[String]()
    verify()
  }

  // Define function to load RDD[Rating]
  def loadFile(file:RDD[String]):RDD[Rating] = {
    file.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    })
  }

  // Define case class for loaded Data
  case class Data(train: RDD[Rating],test:RDD[Rating])

 // Define Loading Data function
  def loadData(conf:Conf,spark:SparkSession)={
    println("Loading training data from: " + conf.train())
    val trainFile = spark.sparkContext.textFile(conf.train())
    val train = loadFile(trainFile)
    assert(train.count == 80000, "Invalid training data")

    println("Loading test data from: " + conf.test())
    val testFile = spark.sparkContext.textFile(conf.test())
    val test = loadFile(testFile)
    assert(test.count == 20000, "Invalid test data")
    Data(train,test)
  }

  // ------------------------------- Milestone 1 equations ------------------------------- //
  // Equation 3
  def scale(x: Double, v: Double): Double = if (x == v) 1 else if (x > v) 5 - v else v - 1
  // Equation 2
  def normalize(x: Double, v: Double): Double = (x - v) / scale(x, v)
  // Equation 5
  def predict(x: Double, y: Double): Double = x + y * scale(x + y, x)

  // Define the absolute error
  def absErr(x: Double, y: Double): Double = (x - y).abs
  // Define Round Function
  def round(x: Double, n: Int):Double = {
    val sig = math.pow(10,n)
    (x * sig).round/sig
  }
  // ------------------------------------------------------------------------------------- //

  // Define function to compute normalized deviation
  def computeNormalizedDeviation(train: RDD[Rating],averageUserRatingsPerUser:RDD[(Int, Double)]):RDD[Rating] = {
    train.map(x => (x.user, (x.item, x.rating)))
      .join(averageUserRatingsPerUser)
      .map{case (user, ((item, rating), avgRating)) => Rating(user, item, normalize(rating, avgRating))}
  }

  // Define function to compute MAE
  def computeMae(prediction: RDD[ExtendedRating]): Double = {
    // Compute MAE
    round(prediction.map(r => absErr(r.pred, r.groundTruthRating)).mean(),4)
  }

  // Define a case class for Running time calculations
  case class Statistics(min: Double, max: Double, avg: Double, std: Double)

  // Define function to compute Min Max Mean Std of a List of doubles
  def computeStatistics(l: List[Double]):Statistics = {
    val mean = l.sum / l.size
    val std = scala.math.sqrt(l.map(x => scala.math.pow(x - mean, 2)).sum / l.size)
    Statistics(l.min, l.max, mean, std)
  }

  // Define a function to compute the averageRatings for either a user or item using pattern matching
  def computeAverageRating(data: RDD[Rating], option: String): RDD[(Int, Double)] = option match {
    case "user" => data.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
    case "item" => data.map(x => (x.item, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
  }

  // Define a function to preprocess ratings (Equation 4, Milestone 2)
  def preprocessRatings(ratingNormalizedDev:RDD[Rating]):RDD[Rating] = {
    // Compute the norm of the normalized rating with respect to each user
    val normUserNormalizedDev = ratingNormalizedDev.map(x => (x.user,math.pow(x.rating,2)))
      .reduceByKey(_+_).mapValues(math.sqrt).collectAsMap()

    // Compute the adjusted ratings
    val adjustedRatings = ratingNormalizedDev.map(x => Rating(x.user,x.item,x.rating/normUserNormalizedDev(x.user)))
    adjustedRatings
  }

  // Define a function to compute similarity between two users
  // This function can compute both the "cosineSimilarity" and "jaccardSimilarity"
  def computeSimilarityUVCosine(u:Int,v:Int,ItemsPerUserMap: scala.collection.Map[Int,Set[Int]]
                                ,ratings: scala.collection.Map[(Int, Int), Double]):(Double,Int) ={
    // Get Set of Items for user 1
    val ItemsU = try{ItemsPerUserMap(u)}catch{case _: Throwable =>
      //println(s"Empty items for u $u")
      Set.empty[Int]}
    // Get Set of Items for user 2
    val ItemsV = try{ItemsPerUserMap(v)}catch{case _: Throwable =>
      //println(s"Empty items for v $v")
      Set.empty[Int]}
    // Compute common Items
    val commonItems = ItemsU.intersect(ItemsV)
    // foldLeft handles empty sets
    //  Compute "Equation 5, Milestone 2" + number of common items which reflects the number of multiplications
    (commonItems.foldLeft(0.0){case (acc,item) => acc+ratings(u,item)*ratings(v,item) },commonItems.size)
  }

  def computeSimilarityUVJaccard(u:Int,v:Int,ItemsPerUserMap: scala.collection.Map[Int,Set[Int]]
                          ,ratings: scala.collection.Map[(Int, Int), Double]):(Double,Int) ={
    // Get Set of Items for user 1
    val ItemsU = try{ItemsPerUserMap(u)}catch{case _: Throwable =>
      //println(s"Empty items for u $u")
      Set.empty[Int]}
    // Get Set of Items for user 2
    val ItemsV = try{ItemsPerUserMap(v)}catch{case _: Throwable =>
      //println(s"Empty items for v $v")
      Set.empty[Int]}
    // Compute common Set
    val commonItems = ItemsU.intersect(ItemsV)

    // Compute jaccard coefficient (intersection size over union size of items)
    val allItems = ItemsU.union(ItemsV)
    // Avoid division by 0 of there is somehow two empty sets (which should be impossible)
    if (allItems.nonEmpty) {
      (commonItems.size.toDouble/allItems.size,0)
    }else (0.0,0)

  }

  def computeSuv(u:Set[Int],preprocessedRatings:RDD[Rating],method:String):RDD[Similarity] = {
    // Compute all pairs of users. u is the set of users in the testing set
    // We get v from the preprocessed training set
    val uv = preprocessedRatings.map(_.user).distinct
      .flatMap(v => u.map(u => (u,v)))

    // Compute items of each users and collect them as Map (for fast computation)
    val itemPerUser = preprocessedRatings.map( r => (r.user,r.item))
      .groupByKey().mapValues(x => x.toSet).collectAsMap()

    // Similarly, we collect the ratings as map. The key is the pair (user,item)
    val ratingsMap = preprocessedRatings.map(r => ((r.user,r.item),r.rating)).collectAsMap()
//    val tmp1 = preprocessedRatings.map{case Rating(user, item, rating) => (user,(item,rating))}
//    val tmp2 = preprocessedRatings.map{case Rating(user, item, rating) => (item,(user,rating))}
    // Compute the Suv for all pairs uv
    val Suv = method match {

      case "cosineSimilarity" =>

        uv.map{case (u,v) => (u,v,computeSimilarityUVCosine(u,v,itemPerUser,ratingsMap))}
        .map{case (u,v,(similarity,count))=> Similarity(u,v,similarity,count)}

      case "jaccardSimilarity" => uv.map{case (u,v) => (u,v,computeSimilarityUVJaccard(u,v,itemPerUser,ratingsMap))}
        .map{case (u,v,(similarity,count))=> Similarity(u,v,similarity,count)}

      case _  => throw new Exception("Similarity Method Not Found")
    }

    Suv
  }

  // Compute User Deviation of a user for a given item (Equation 2, Milestone 2)
  def computeUserDeviation(u:Int, item:Int, Suv:scala.collection.Map[(Int,Int),Double],
                           UserPerItem:scala.collection.Map[Int,Set[Int]]
                          ,rating: scala.collection.Map[(Int,Int),Double]):Double={
    // Try to collect the set of users for item i (We may have an error if the item is not present and training set)
    val setOfUsers = try{UserPerItem(item)}catch{case _: Throwable =>
      //println(s"Empty users for item $item")
      Set.empty[Int]}
    // Compute user deviation if this set is not empty else return 0
    val userDev = setOfUsers.map(v =>
      {// If the we don't have similarity return 0 (We may have this issue in knn)
        val suv = Suv.getOrElse((u,v),0.0)
        val rv = rating(v,item)
        (rv*suv,suv.abs)
      })
      // foldLeft handles empty sets. We don't need to check that
      .foldLeft((0.0,0.0)) {case((num,deNum),(weight,absSim))=>(num + weight,deNum + absSim)}

      if (userDev._2!=0)  userDev._1/userDev._2 else 0.0

  }

  // Define Prediction class to store both similarities and prediction
  case class Prediction(Pred:RDD[ExtendedRating],Suv:RDD[Similarity])

  def computeUserSpecificPrediction(train:RDD[Rating],test:RDD[Rating],method:String):Prediction = {

    // Compute global average
    val globalAvg = train.map(_.rating).mean()

    // Compute users' average ratings
    val averageUserRatingsPerUser = computeAverageRating(train, "user")

    // Compute Normalized Deviation
    val normalizedDeviation = computeNormalizedDeviation(train, averageUserRatingsPerUser)

    // Preprocess Ratings
    val ratings = preprocessRatings(normalizedDeviation)

    // Get the set of users u form the training data
    // If we want to compute only similarities we need for prediction we can pass only users of the testing set
    val u = train.map(_.user).distinct().collect().toSet

    // Compute Suv
    val Suv = computeSuv(u,ratings,method)

    // Collect Suv as map (for fast computation). The key is the pair (u,v)
    val SuvMap = Suv.map(s => ((s.u,s.v),s.similarity)).collectAsMap()

    // Similarly we collect normalized deviation. The is the pair (user,item)
    val normalizedDeviationMap = normalizedDeviation.map(r =>((r.user,r.item),r.rating)).collectAsMap()

    // Same. Key is the user
    val userAvgMap = averageUserRatingsPerUser.collectAsMap()

    // We compute and collect users for each item
    val usersPerItem = ratings.map( r => (r.item,r.user))
      .groupByKey().mapValues(x => x.toSet).collectAsMap()

    // Compute Prediction
    val prediction =
      test.map{
        case Rating(user, item, gt) =>
        (user,item, userAvgMap.getOrElse(user,globalAvg),
        computeUserDeviation(user,item,SuvMap,usersPerItem,normalizedDeviationMap),
        gt)
      }.map{case (user,item,userAvg,userDev,gt) => ExtendedRating(user,item,predict(userAvg,userDev),gt)}

//      test.map(r => ExtendedRating(r.user,r.item,
//      predict(
//        // if a user is not present in the training set we his average rating by the global average
//        try{userAvgMap(r.user)}catch{case _ : Throwable => globalAvg },
//      computeUserDeviation(r.user,r.item,SuvMap,usersPerItem,normalizedDeviationMap)
//    ),r.rating))
    Prediction(prediction,Suv)
  }

  // Define a class to store MemoryUsage
  case class MemoryUsage(total: Int, nonZero: Int)

  // Compute memory usage for a list of doubles
  def computeMemoryUsage(l:List[Double]):MemoryUsage = {
    // Count non zeros
    val nonZeros = l.count(x => x != 0.0)
    val byteNonZeros = nonZeros*64/8
    println(s"Total Memory to store Suv = ${64/8*l.size}")
    MemoryUsage(64/8*l.size,byteNonZeros)
  }

  // Define a class to store similarity metrics
  case class SimilarityMetrics(size: Long, SuvComputation: Int, similarityStatistics: Statistics, memory: MemoryUsage)

  // Compute Similarity Metrics
  def computeSimilarityMetrics(Suv:RDD[Similarity]): SimilarityMetrics = {

    val setOfUsers = Suv.map(_.u).distinct().collect().toSet.union(Suv.map(_.v).distinct().collect().toSet)
    println(s"Set of users in ml-100k dataset set = ${setOfUsers.size}")
    // Q2.3.3
    val SuvComputation = Suv.count().toInt
    // Q2.3.4
    val SimStat = computeStatistics(Suv.map(_.count).collect().toList.map(x => x.toDouble))
    // Q2.3.5
    val MemoryUsage = computeMemoryUsage(Suv.map(_.similarity).collect().toList)

    SimilarityMetrics(Suv.count(),SuvComputation,SimStat,MemoryUsage)
  }

  // Define a class to store all the results for Exercise2.3
  case class Exercise2(maeCosineSim:Double,maeJaccardSim:Double, suvTimeStat:Statistics,
                         predTimeStat:Statistics,similarityMetrics:SimilarityMetrics)

  def exercise2(conf: Conf, spark:SparkSession):Exercise2 = {
    // Define an emptyList
    var elapsedTimesPred = List[Double]()
    var elapsedTimesSim = List[Double]()
    var pred = spark.sparkContext.emptyRDD[ExtendedRating]
    var Suv = spark.sparkContext.emptyRDD[Similarity]

    var dtSim = 0.0
    var dtPred = 0.0

    val data = loadData(conf, spark)

    // Iterate 10 times
    for (i <- 1 to 5) {
      // Clear Cache
      spark.sqlContext.clearCache()
      // Load Data
      println(s"*** Iteration = $i ***")

      val begin = System.nanoTime()

      // Compute prediction
      val prediction = computeUserSpecificPrediction(data.train,data.test,"cosineSimilarity")
      Suv = prediction.Suv
      // Evaluate Similarity
      Suv.map(_.similarity).sum()
      dtSim = (System.nanoTime()-begin)*1E-3
      // Evaluate prediction
      pred = prediction.Pred
      pred.map(_.pred).sum()
      dtPred = (System.nanoTime()-begin)*1E-3
      // Append to the list
      elapsedTimesPred = dtPred +: elapsedTimesPred
      elapsedTimesSim = dtSim +: elapsedTimesSim
      println(s"Similarity Time = ${dtSim*1E-6}s")
      println(s"Prediction Time = ${dtPred*1E-6}s")
    }

    // Compute MAE Cosine Similarity
    val maeCosine = round(pred.map(r => absErr(r.pred, r.groundTruthRating)).mean(),4)
    println(s"*** Compute Jaccard ***")
    val begin = System.nanoTime()
    // Compute MAE Jaccard Similarity
    val maeJaccard = round(computeUserSpecificPrediction(data.train,data.test,"jaccardSimilarity").Pred
      .map(r => absErr(r.pred, r.groundTruthRating)).mean(),4)
    println(s"Jaccard Time = ${(System.nanoTime()-begin)*1E-9}s")

    // Compute Similarity Metrics
    val similarityMetrics = computeSimilarityMetrics(Suv)
    // Return Results
    Exercise2(maeCosine, maeJaccard, computeStatistics(elapsedTimesSim),
      computeStatistics(elapsedTimesPred),similarityMetrics)
  }


  def computeRank(usersList:List[(Int,Double)]):List[(Int,Int,Double)] = {
    usersList.sortWith(_._2 > _._2).zipWithIndex.map{case ((v,similarity),index) => (index,v,similarity)}
  }

  case class ExtendedSimilarity(index:Int,u:Int,v:Int,similarity: Double)

  def computeNeighboursRank(Suv: RDD[Similarity]):RDD[ExtendedSimilarity] = {
    Suv.map(s => (s.u,(s.v,s.similarity))).groupByKey()
        .mapValues(x => computeRank(x.toList))
        .flatMap{case (u,list) => list
        .map{case (index,v,similarity) => ExtendedSimilarity(index,u,v,similarity)}}
  }

  case class PredictionNN(predictions: Map[Int,RDD[ExtendedRating]], SuvNN: RDD[ExtendedSimilarity])
  def computePredictionForKnn(train:RDD[Rating],test:RDD[Rating],kList:List[Int]):PredictionNN = {

    // Compute global average
    val globalAvg = train.map(_.rating).mean()

    // Compute users' average ratings
    val averageUserRatings = computeAverageRating(train, "user")

    // Compute Normalized Deviation
    val normalizedDeviation = computeNormalizedDeviation(train, averageUserRatings)

    // Preprocess Ratings
    val ratings = preprocessRatings(normalizedDeviation)

    // Get the set of users u form the training data
    // If we want to compute only similarities we need for prediction we can pass only users of the testing set
    val u = train.map(_.user).distinct().collect().toSet

    // Compute Suv
    val Suv = computeSuv(u,ratings,"cosineSimilarity")
      // remove autoSimilarity
      .filter{case Similarity(u, v, similarity, count) => u!=v}

    // ComputeKNN
    val SuvNN = computeNeighboursRank(Suv).cache

    // We collect normalized deviation. The is the pair (user,item)
    val normalizedDeviationMap = normalizedDeviation.map(r =>((r.user,r.item),r.rating)).collectAsMap()

    // Same. Key is the user
    val userAvgMap = averageUserRatings.collectAsMap()

    // We compute and collect users for each item
    val usersPerItem = ratings.map( r => (r.item,r.user))
      .groupByKey().mapValues(x => x.toSet).collectAsMap()

    var kPrediction = Map.empty[Int,RDD[ExtendedRating]]

    kList.map(k => k -> {
      println(s"*** Nearest ${k} ***")
      val kSuv = SuvNN.filter(_.index<k)

      // Collect Suv as map (for fast computation). The key is the pair (u,v)
      val SuvMap = kSuv.map(s => ((s.u,s.v),s.similarity)).collectAsMap()

      // Compute Prediction
      println("Compute Prediction")

      val prediction =
        test.map{
          case Rating(user, item, gt) =>
            (user,item, userAvgMap.getOrElse(user,globalAvg),
              computeUserDeviation(user,item,SuvMap,usersPerItem,normalizedDeviationMap), gt)
        }.map{case (user,item,userAvg,userDev,gt) => ExtendedRating(user,item,predict(userAvg,userDev),gt)}

        kPrediction  += (k->prediction)
    }).toMap

    PredictionNN(kPrediction,SuvNN)
  }

  case class Exercise3(listMae: Map[Int,Double],lowestK:Int,deltaBaseline:Double,memoryUsage:Map[Int,Int],
                      storableSim:Long,availableRam:Long)
  def exercise3(conf: Conf, spark:SparkSession):Exercise3= {

    val data = loadData(conf,spark)

    val kList = List(10, 30, 50, 100, 200, 300, 400, 800, 943)
    val maeBaseline = 0.7669
    val predictionNN = computePredictionForKnn(data.train, data.test, kList)
    val listPredictionMae = predictionNN.predictions.map{case (k,pred) => (k,computeMae(pred))}

    val MaeThanBaseline = listPredictionMae.toList.filter(x => x._2<maeBaseline).min

    val lowestKWithBetterMaeThanBaseline = MaeThanBaseline._1
    val differenceToBaseline = round(MaeThanBaseline._2-maeBaseline,4)

    val memoryUsageList = kList.map( k => (k,
      {
        val kSuv = predictionNN.SuvNN.filter(_.index<k)
        (kSuv.count()*64/8).toInt
      })).toMap

    val availableRam = (16 * math.pow(1024,3)).toLong
    val storableSim = availableRam/(lowestKWithBetterMaeThanBaseline*3*8)

    Exercise3(listPredictionMae,lowestKWithBetterMaeThanBaseline,differenceToBaseline,
      memoryUsageList,storableSim,availableRam)
  }

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

  def retrieveTop(prediction:RDD[ExtendedRating],itemName:scala.collection.Map[Int,String],top:Int):Array[List[Any]] = {
    val topResults= prediction.map( x => (x.item,x.pred)).sortBy(x => (x._2,-x._1),false).take(top)
    topResults.map( x=> List(x._1,itemName.getOrElse(x._1,"Movie not found"),x._2))
  }


  def recommend(data:RDD[Rating], personalFile:RDD[String]) = {

    val kList = List(30,300)
    // Parsing the personal.csv using our function
    val personalRDD = personalFile.map(x => convertToRatings(x,','))

    // Get Rated items and convert them to ratings
    val rated = personalRDD.filter(r => r.rating.isDefined).map(x => Rating(x.user,x.item,x.rating.get))

    // Get notRated items and convert them to ratings (we set the rating at 0)
    val notRated =  personalRDD.filter(r => r.rating.isEmpty).map(x => Rating(x.user,x.item,0))

    // Get item Names and collect them as Map
    val itemsName = personalRDD.map(r => (r.item,r.item_name)).collectAsMap()

    // Construct our training data by adding our ratings
    val train = data.union(rated)

    val predictions = computePredictionForKnn(train,notRated,kList).predictions

    predictions.map{case (k,pred) => (k,retrieveTop(pred,itemsName,5))}
  }
}


