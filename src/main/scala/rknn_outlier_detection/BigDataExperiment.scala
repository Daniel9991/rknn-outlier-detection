package rknn_outlier_detection

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.shared.distance.DistanceFunctions
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import rknn_outlier_detection.big_data.detection.{Antihub, AntihubRefined, AntihubRefinedParams, DetectionStrategy, RankedReverseCount}
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.big_data.search.pivot_based.PkNN
import rknn_outlier_detection.big_data.search.reverse_knn.ReverseNeighborsSearch

object BigDataExperiment {
    def main(args: Array[String]): Unit = {

//        val k = args(0).toInt
        val k = 100

//        val searchMethod = args(1)
        val searchMethod = "pknn"
        if(searchMethod != "exhaustive" && searchMethod != "pknn") throw new Exception(s"Unknown search strategy ${args(1)}")

//        val detectionMethod = args(2)
        val detectionMethod = "antihub"
        if(detectionMethod != "antihub" && detectionMethod != "ranked" && detectionMethod != "refined") throw new Exception(s"Unknown detection strategy ${args(2)}")

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
//        val fullPath = System.getProperty("user.dir")

        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled.csv"
//        val datasetRelativePath = "datasets\\iris-synthetic-2.csv"
        val rocRelativePath = s"rocs\\creditcard_${k}_${searchMethod}_${detectionMethod}.csv"

        val datasetPath = s"${fullPath}\\${datasetRelativePath}"
        val rocPath = s"${fullPath}\\${rocRelativePath}"

        val before = System.nanoTime

        val sc = new SparkContext(new SparkConf().setAppName("Sparking2"))
//        val sc = new SparkContext(new SparkConf().setMaster("local[*]").set("spark.default.parallelism", "16").setAppName("Sparking2"))

//        val rawData = ReaderWriter.readCSV(filepath, hasHeader=false)
        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
        val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            val classification = if(line.last == "1") "1.0" else "0.0"
            (new Instance(index.toString, attributes), classification)
        })
        instancesAndClassification.cache()
        val instances = instancesAndClassification.map(_._1)
        println(s"There are ${instances.count} instances")

//        val instances = sc.parallelize(instancesArray)
//        println(s"---------------There are ${instances.count} read by textFile--------------")
//        sc.stop()
//        instances.cache()
//        val searchBefore = System.nanoTime
        val kNeighbors = searchMethod match {
            case "pknn" => new PkNN(280).findKNeighbors(instances, k, euclidean, sc)
            case "exhaustive" =>  new ExhaustiveBigData().findKNeighbors(instances, k, euclidean, sc)
        }
        kNeighbors.count()
//        kNeighbors.cache()
//        kNeighbors.count()
//        val searchAfter = System.nanoTime
//        val rNeighbors = ReverseNeighborsSearch.findReverseNeighbors(kNeighbors)
//        rNeighbors.cache()
//        rNeighbors.count()
//        val reverseAfter = System.nanoTime
//        val detectionResult = detectionMethod match {
//            case "antihub" => new Antihub().antihub(rNeighbors)
//            case "ranked" => new RankedReverseCount(k).calculateAnomalyDegree(rNeighbors, k)
//            case "refined" => new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors)
//        }
////        detectionResult.count()
//        val after = System.nanoTime
//
//        val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
//        val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
//        println(predictionsAndLabels.collect().mkString("Array(\n\t", ",\n\t ", ")"))
//        val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
//        val elapsedTime = s"${(after - before) / 1000000}ms"
////        val setupTime = s"${(searchBefore - before) / 1000000}ms"
////        val knnTime = s"${(searchAfter - searchBefore) / 1000000}ms"
////        val reverseTime = s"${(reverseAfter - searchAfter) / 1000000}ms"
////        val detectionTime = s"${(after - reverseAfter) / 1000000}ms"
//
//        println(s"-----------------------------------\nArea under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}\n-----------------------------------")
////        println(s"-----------------------------------\nElapsed time: $elapsedTime\nSetup time: $setupTime\nSearch time: $knnTime\nReverse time: $reverseTime\nDetection time: $detectionTime\n-----------------------------------")
////        println(s"-----------------------------\nPartitions $partitionsAmount\nTime: $elapsedTime\nFinal partitions: ${detectionResult.partitions.length}")
////        System.in.read()
////        sc.stop()
//        val roc = detectionMetrics.roc().collect()
//        val rocTextForCSV = roc.map(tuple => s"${tuple._1},${tuple._2}").mkString("\n")
//        ReaderWriter.writeToFile(rocPath, rocTextForCSV)
//        val resultsFileId = s"${searchMethod}_${detectionMethod}_${k}"
//        saveStatistics(resultsFileId, detectionMetrics.areaUnderROC(), detectionMetrics.areaUnderPR(), s"$elapsedTime")
        System.in.read()
        sc.stop()
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
