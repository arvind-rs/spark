package ml

/*
 * Simple application to learn to use Spark's MLlib component
 * @author: ArvindRS
 * @date: 07/26/2017
 */

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

// Define the flight data model
case class Flight(dofM: String, dofW: String, carrier: String, tailnum: String, flnum: Int, org_id: String, origin: String, dest_id: String, dest: String, crsdeptime: Double, deptime: Double, depdelaymins: Double, crsarrtime: Double, arrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Int)

object FlightDelayPredictor {

  def main(args: Array[String]) {

    // Create a Spark context object
    val sc = new SparkContext(new SparkConf().setAppName("Spark ML example").setMaster("local[2]").set("spark.executor.memory", "1g"))

    // Read the csv file into an inputRDD
    val inputRDD = sc.textFile("/tmp/ml_dataset/flight_delay_prediction/flight_details_1.csv")

    println(inputRDD.first())
    println(inputRDD.count())

    // Parse each line of the csv data into Flight data
    val flightRDD = inputRDD.filter(x => !x.contains("DAY_OF_MONTH") && !x.contains("\"\"") && !x.contains(",,")).map(x => parseRow(x)).cache()
    println(flightRDD.first())
    println(flightRDD.count())

    var carrierMap = Map[String, Int]()
    var i = 0
    flightRDD.map(x => x.carrier).distinct().collect().foreach(x => { carrierMap += x -> i; i += 1 })
    println(carrierMap.mkString)
    var originMap: Map[String, Int] = Map()
    var index1: Int = 0
    flightRDD.map(flight => flight.origin).distinct.collect.foreach(x => { originMap += (x -> index1); index1 += 1 })
    var destMap: Map[String, Int] = Map()
    var index2: Int = 0
    flightRDD.map(flight => flight.dest).distinct.collect.foreach(x => { destMap += (x -> index2); index2 += 1 })

    // Create a RDD to store the feature vectors containing important features from Flight model and the label {delayed, not delayed}
    val featureVectorRDD = flightRDD.map(flight => {
      val monthday = flight.dofM.toInt - 1
      val weekday = flight.dofW.toInt - 1 // category
      val crsdeptime1 = flight.crsdeptime.toInt
      val crsarrtime1 = flight.crsarrtime.toInt
      val carrier1 = carrierMap(flight.carrier) // category
      val crselapsedtime1 = flight.crselapsedtime.toDouble
      val origin1 = originMap(flight.origin) // category
      val dest1 = destMap(flight.dest) // category
      val delayed = if (flight.depdelaymins.toDouble > 40) 1.0 else 0.0
      Array(delayed.toDouble, monthday.toDouble, weekday.toDouble, crsdeptime1.toDouble, crsarrtime1.toDouble, carrier1.toDouble, crselapsedtime1.toDouble, origin1.toDouble, dest1.toDouble)
    })

    println(featureVectorRDD.first().mkString)

    // Create training rows by specifying the label Y and input variables Xs
    val labelledDataRDD = featureVectorRDD.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))
    println(labelledDataRDD.first())

    val mldata0 = labelledDataRDD.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(1)
    val mldata1 = labelledDataRDD.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1
    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    println(testData.take(1))

    // Fitting the model
    var categoricalFeaturesInfo = Map[Int, Int]()
    categoricalFeaturesInfo += (0 -> 31)
    categoricalFeaturesInfo += (1 -> 7)
    categoricalFeaturesInfo += (4 -> carrierMap.size)
    categoricalFeaturesInfo += (6 -> originMap.size)
    categoricalFeaturesInfo += (7 -> destMap.size)

    val numClasses = 2
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 7000
    // Fit and train the model
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    println(model.toDebugString)

    // Testing the model
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val wrongPrediction = (labelAndPreds.filter {
      case (label, prediction) => (label != prediction)
    })

    val ratioWrong = wrongPrediction.count().toDouble / testData.count()
    println("wrong prediction %: " + ratioWrong)
    println("wrong prediction count: " + wrongPrediction.count())
  }

  // Function to parse each line into the Flight data model
  def parseRow(input: String): Flight = {
    val line = input.replaceAll("\"", "").split(",")
    try {
      return Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5), line(6), line(8), line(9), line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toDouble, line(17).toDouble, line(18).toDouble.toInt)
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
    }
  }
}
