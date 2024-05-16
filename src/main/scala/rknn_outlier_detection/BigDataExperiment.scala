package rknn_outlier_detection

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.shared.distance.DistanceFunctions
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import rknn_outlier_detection.big_data.detection.{Antihub, DetectionStrategy}
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.big_data.search.reverse_knn.ReverseNeighborsSearch

object BigDataExperiment {
    def main(args: Array[String]): Unit = {

        bigDataExperiment()
    }

    def bigDataExperiment(): Unit = {
        // crear context
        // cargar los datos
        // crear y paralelizar instancias
        // ejecutar la búsqueda de knn
        // ejecutar la búsqueda de rknn
        // ejecutar la detección

        val before = System.nanoTime

        val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

        val datasetFilename = "datasets/iris-synthetic-2.csv"
        val distanceFunction = distFun
//        val k = 10
//        val k = 20
//        val k = 30
        val k = 40

        val rawData = ReaderWriter.readCSV(datasetFilename, hasHeader=false)
        val instancesArray = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            val classification = if(line.last == "Iris-setosa") "1.0" else "0.0"
            new Instance(index.toString, attributes, classification=classification)
        })

        val instances = sc.parallelize(instancesArray)
        val kNeighbors = ExhaustiveBigData.findKNeighbors(instances, k, distanceFunction, sc)
        val rNeighbors = ReverseNeighborsSearch.findReverseNeighbors(kNeighbors)
        val detectionResult = Antihub.antihub(rNeighbors)

        val after = System.nanoTime

        val predictionsAndLabels = instances.map(instance => (instance.id, instance.classification)).join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
        val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
        println(s"Area under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}")

        println(s"Elapsed time: ${(after - before) / 1000000}ms")
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
        def findNeighborsForDataset(
        datasetFilename: String,
        savedResultsFilename: String,
        hasHeader: Boolean,
        k: Int,
        distanceFunction: DistanceFunction,
        sc: SparkContext
    ): Unit = {

        val rawData = ReaderWriter.readCSV(datasetFilename, hasHeader=hasHeader)
        val instances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            new Instance(index.toString, attributes, classification="")
        })

        val neighbors = ExhaustiveBigData.findKNeighbors(
            sc.parallelize(instances),
            k,
            distanceFunction,
            sc
        )

        val sortedResults = neighbors.collect().sortWith((tuple1, tuple2) => tuple1._1.toInt < tuple2._1.toInt)
        val stringifiedNeighbors = sortedResults.map(tuple =>
                s"${tuple._1},${tuple._2.map(neighbor => s"${neighbor.id};${neighbor.distance}").mkString(",")}"
        )

        println(sortedResults.map(_._1).mkString("Array(", ", ", ")"))

        val toWrite = stringifiedNeighbors.mkString("\n")
        ReaderWriter.writeToFile(
            savedResultsFilename,
            toWrite
        )

        println("Done saving results")
    }
}
