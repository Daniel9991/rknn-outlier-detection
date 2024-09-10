package rknn_outlier_detection

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.shared.distance.DistanceFunctions
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import rknn_outlier_detection.big_data.detection.{Antihub, AntihubRefined, AntihubRefinedParams, DetectionStrategy, RankedReverseCount}
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.big_data.search.pivot_based.PkNN
import rknn_outlier_detection.big_data.search.reverse_knn.ReverseNeighborsSearch
import rknn_outlier_detection.small_data.search.pivot_based.{FarthestFirstTraversal, PersistentRandom}

// spark-submit --class rknn_outlier_detection.BigDataExperiment --master local[8] --driver-memory 12g --conf spark.default.parallelism=240 --conf spark.memory.storageFraction=0.3 C:\Users\danny\OneDrive\Escritorio\Proyectos\scala\rknn-outlier-detection\target\scala-2.13\rknn-outlier-detection_2.13-1_2.jar

object BigDataExperiment {
    def maina(args: Array[String]): Unit = {

        val k = 800
        val pivotsAmount = 100
//        val detectionMethod = "antihub"
//        val detectionMethod = "ranked"
        val detectionMethod = "refined"
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val onStart = System.nanoTime

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toString, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)

            val pivots = instances.takeSample(withReplacement = false, pivotsAmount)

            val kNeighbors = new PkNN(pivots, 1000).findApproximateKNeighbors(instances, k, euclidean, sc).cache()

            //                    if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0){
            //                        throw new Exception("There are element with null neighbors")
            //                    }

//            kNeighbors.count()
//            val onFinishSearch = System.nanoTime
//            val searchDuration = (onFinishSearch - onStart) / 1000000
//
//            val slicedKNeighbors = kNeighbors.mapValues(arr => arr.slice(0, k))
//
//            val onReverse = System.nanoTime
            val rNeighbors = ReverseNeighborsSearch.findReverseNeighbors(kNeighbors).cache()
//            rNeighbors.count()
//            val onFinishReverse = System.nanoTime
//            val reverseDuration = (onFinishReverse - onReverse) / 1000000
//
//
//            val onDetection = System.nanoTime

            val detectionResult = (detectionMethod match {
                case "antihub" => new Antihub().antihub(rNeighbors)
                case "ranked" => new RankedReverseCount(k).calculateAnomalyDegree(rNeighbors, k)
                case "refined" => new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors)
            }).cache

            detectionResult.count()
            val onFinishDetection = System.nanoTime
//            val detectionDuration = (onFinishDetection - onDetection) / 1000000

            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
            val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val elapsedTime = s"${(onFinishDetection - onStart) / 1000000}ms"

            println(s"-----------------------------------\nArea under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}\nElapsed time: $elapsedTime-----------------------------------")

            val rocRelativePath = s"rocs\\creditcard${if(datasetSize == -1) "" else s"_${datasetSize}"}_${pivotsAmount}_${k}_pknn_approximate_1_${detectionMethod}.csv"
            val rocPath = s"${fullPath}\\${rocRelativePath}"

            val roc = detectionMetrics.roc().collect()
            val rocTextForCSV = roc.map(tuple => s"${tuple._1},${tuple._2}").mkString("\n")
            ReaderWriter.writeToFile(rocPath, rocTextForCSV)
            val resultsFileId = s"${if(datasetSize == -1) "full" else s"${datasetSize}"}_${pivotsAmount}_${k}_pknn_approximate_${detectionMethod}"
            saveStatistics(resultsFileId, detectionMetrics.areaUnderROC(), detectionMetrics.areaUnderPR(), elapsedTime)

            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def main(args: Array[String]): Unit = {

        val kValues = Array(1200)
        val pivotsAmounts = Array(12)
        val detectionMethods = Array("antihub", "ranked", "refined")
//        val detectionMethods = Array("refined")
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val onStart = System.nanoTime

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toString, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)

            val maxK = kValues.max

            pivotsAmounts.foreach(pivotsAmount => {
                val pivots = instances.takeSample(withReplacement = false, pivotsAmount) // seed=87654

                val kNeighbors = new PkNN(pivots, 1000).findApproximateKNeighbors(instances, maxK, euclidean, sc).cache()
//                val kNeighbors = new ExhaustiveBigData().findKNeighbors(instances, maxK, euclidean, sc).cache()

                //                    if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0){
                //                        throw new Exception("There are element with null neighbors")
                //                    }

                kNeighbors.count()
                val onFinishSearch = System.nanoTime
                val searchDuration = (onFinishSearch - onStart) / 1000000

                kValues.foreach(k => {
                    val slicedKNeighbors = kNeighbors.mapValues(arr => arr.slice(0, k))

                    val onReverse = System.nanoTime
                    val rNeighbors = ReverseNeighborsSearch.findReverseNeighbors(slicedKNeighbors).cache()
                    rNeighbors.count()
                    val onFinishReverse = System.nanoTime
                    val reverseDuration = (onFinishReverse - onReverse) / 1000000

                    detectionMethods.foreach(detectionMethod => {

                        val onDetection = System.nanoTime

                        val detectionResult = (detectionMethod match {
                            case "antihub" => new Antihub().antihub(rNeighbors)
                            case "ranked" => new RankedReverseCount(k).calculateAnomalyDegree(rNeighbors, k)
                            case "refined" => new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors)
                        }).cache

                        detectionResult.count()
                        val onFinishDetection = System.nanoTime
                        val detectionDuration = (onFinishDetection - onDetection) / 1000000

                        val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
                        val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
                        val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

                        val elapsedTime = s"s: ${searchDuration} - r: ${reverseDuration} - d: ${detectionDuration} = ${searchDuration + reverseDuration + detectionDuration}ms"

                        println(s"-----------------------------------\nArea under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}\nElapsed time: $elapsedTime-----------------------------------")

                        val rocRelativePath = s"rocs\\creditcard${if(datasetSize == -1) "" else s"_${datasetSize}"}_${pivotsAmount}_${k}_pknn_approximate_1_${detectionMethod}.csv"
                        val rocPath = s"${fullPath}\\${rocRelativePath}"

                        val roc = detectionMetrics.roc().collect()
                        val rocTextForCSV = roc.map(tuple => s"${tuple._1},${tuple._2}").mkString("\n")
                        ReaderWriter.writeToFile(rocPath, rocTextForCSV)
                        val resultsFileId = s"${if(datasetSize == -1) "full" else s"${datasetSize}"}_${pivotsAmount}_${k}_pknn_approximate_${detectionMethod}"
                        saveStatistics(resultsFileId, detectionMetrics.areaUnderROC(), detectionMetrics.areaUnderPR(), elapsedTime)
                    })
                })
            })
            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def withDatasetExperiment(args: Array[String]): Unit ={
        try{

//            val k = args(0).toInt
                    val k = 100

            //        val searchMethod = args(1)
            val searchMethod = "pknn"
            if(searchMethod != "exhaustive" && searchMethod != "pknn") throw new Exception(s"Unknown search strategy ${args(1)}")

//            val detectionMethod = args(2)
                    val detectionMethod = "antihub"
            if(detectionMethod != "antihub" && detectionMethod != "ranked" && detectionMethod != "refined") throw new Exception(s"Unknown detection strategy ${args(2)}")

            val pivotsAmount = args(3).toInt

            val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
//            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled.csv"
            //        val datasetRelativePath = "datasets\\iris-synthetic-2.csv"
            val rocRelativePath = s"rocs\\creditcard_${k}_${searchMethod}_${detectionMethod}.csv"

            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val before = System.nanoTime

            val spark = SparkSession.builder()
                        .appName("ProcessingDatasets")
                        .master("local[*]")
                        .getOrCreate();

            var df = spark.read.format("csv")
                .option("header", "false")
                .load(datasetPath);

            df.show(5)
            df.printSchema()

            //        val rawData = ReaderWriter.readCSV(filepath, hasHeader=false)
//            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
//            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
//                val (line, index) = tuple
//                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//                val classification = if (line.last == "1") "1.0" else "0.0"
//                (new Instance(index.toString, attributes), classification)
//            })
//            //            }).sample(withReplacement = false, 0.1)
//            instancesAndClassification.cache()
//            val instances = instancesAndClassification.map(_._1)

            //        val instances = sc.parallelize(instancesArray)
            //        println(s"---------------There are ${instances.count} read by textFile--------------")
            //        sc.stop()
            //        instances.cache()
            //        val searchBefore = System.nanoTime
//            val kNeighbors = searchMethod match {
//                case "pknn" => new PkNN(pivotsAmount).findKNeighbors(instances, k, euclidean, sc)
//                case "exhaustive" =>  new ExhaustiveBigData().findKNeighbors(instances, k, euclidean, sc)
//            }
//            kNeighbors.count()
//            println(s"There are ${instances.count} instances")
//
//            println("---------------Done executing-------------------")
//            System.in.read()
//            sc.stop()
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def saveStatistics(testId: String, auroc: Double, auprc: Double, time: String): Unit = {
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\creditcard-results.csv"
        val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
        val updatedRecords = s"${previousRecordsText}\n$testId,$auroc,$auprc,$time"
        ReaderWriter.writeToFile(filename, updatedRecords)
    }

//    def syntheticIrisExperiment(): Unit = {
//        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Sparking2"))
//        val datasetFilename = "datasets/iris-synthetic-2.csv"
//        val savedResultsFilename = "testingDatasets/iris-synthetic-2-neighbors.csv"
//        val k = 10
//        val hasHeader = false
//        val distanceFunction: DistanceFunction = DistanceFunctions.euclidean
//
//        val rawData = ReaderWriter.readCSV(datasetFilename, hasHeader=hasHeader)
//        val instances = rawData.zipWithIndex.map(tuple => {
//            val (line, index) = tuple
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            val classification = if(line.last == "Iris-setosa") "1.0" else "0.0"
//            new Instance(index.toString, attributes, classification=classification)
//        })
//
//        val kNeighbors = ExhaustiveSmallData.findKNeighbors(instances, k, distanceFunction).zipWithIndex.map(tuple => (tuple._2.toString, tuple._1))
//
//        val reverseNeighbors = ReverseNeighborsSmallData.findReverseNeighbors(kNeighbors.map(_._2))
//
//        val rankedResults = rknn_outlier_detection.big_data.detection.RankedReverseCount.calculateAnomalyDegree(sc.parallelize(reverseNeighbors.toSeq), k).collect().sortWith((tuple1, tuple2) => tuple1._1.toDouble < tuple2._1.toDouble)
//        //        val countedResults = Antihub.antihub(sc.parallelize(reverseNeighbors)).collect().sortWith((tuple1, tuple2) => tuple1._1.toDouble < tuple2._1.toDouble)
//        val predictionsAndLabels = sc.parallelize(rankedResults.zip(instances).map(tuple => (tuple._1._2, tuple._2.classification.toDouble)))
//
//        val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
//        println(s"Area under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}")
//
//    }

    def loadNeighborsFromSavedSearch(
        savedResultsFilename: String,
        hasHeader: Boolean
    ): Array[(String, Array[KNeighbor])] = {
        val neighborsRawData = ReaderWriter.readCSV(savedResultsFilename, hasHeader=hasHeader)
        val neighbors = neighborsRawData.map(line => {
            // First element is instance id\
            val id = line(0)
            val kNeighbors = line.slice(1, line.length).map(token => {
                val neighborProperties = token.split(";")
                new KNeighbor(id=neighborProperties(0), distance=neighborProperties(1).toDouble)
            })

            (id, kNeighbors)
        })

        neighbors
    }
//    def findNeighborsForDataset[A](
//        datasetFilename: String,
//        savedResultsFilename: String,
//        hasHeader: Boolean,
//        k: Int,
//        distanceFunction: DistanceFunction[A],
//        sc: SparkContext
//    ): Unit = {
//
//        val rawData = ReaderWriter.readCSV(datasetFilename, hasHeader=hasHeader)
//        val instances = rawData.zipWithIndex.map(tuple => {
//            val (line, index) = tuple
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            new Instance[A](index.toString, attributes, classification="")
//        })
//
//        val neighbors = new ExhaustiveBigData().findKNeighbors(
//            sc.parallelize(instances),
//            k,
//            distanceFunction,
//            sc
//        )
//
//        val sortedResults = neighbors.collect().sortWith((tuple1, tuple2) => tuple1._1.toInt < tuple2._1.toInt)
//        val stringifiedNeighbors = sortedResults.map(tuple =>
//                s"${tuple._1},${tuple._2.map(neighbor => s"${neighbor.id};${neighbor.distance}").mkString(",")}"
//        )
//
//        println(sortedResults.map(_._1).mkString("Array(", ", ", ")"))
//
//        val toWrite = stringifiedNeighbors.mkString("\n")
//        ReaderWriter.writeToFile(
//            savedResultsFilename,
//            toWrite
//        )
//
//        println("Done saving results")
//    }
}
