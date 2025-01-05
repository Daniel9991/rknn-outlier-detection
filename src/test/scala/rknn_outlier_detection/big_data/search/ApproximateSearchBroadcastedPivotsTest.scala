package rknn_outlier_detection.big_data.search

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.BigDataExperiment.saveStatistics
import rknn_outlier_detection.big_data.detection.Antihub
import rknn_outlier_detection.big_data.search.pivot_based.GroupedByPivot
import rknn_outlier_detection.big_data.search.reverse_knn.NeighborsReverser
import rknn_outlier_detection.euclidean
import rknn_outlier_detection.shared.custom_objects.Instance

class ApproximateSearchBroadcastedPivotsTest extends AnyFunSuite{

    test(""){
        val sc = new SparkContext(
            new SparkConf()
                .setMaster("local[*]")
                .setAppName("Test Approximate Search with Broadcasted Pivots")
                .set("spark.executor.memory", "12g")
                .set("spark.default.parallelism", "48")
        )

        val pivotsAmount = 25
        val k = 1000
        val seed = 56342
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))

            val pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed=seed)

            val onStartIter = System.nanoTime
            val kNeighborsIter = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc, tailrec = false).cache()
            kNeighborsIter.count()
            val onFinishIter = System.nanoTime
            val iterDuration = (onFinishIter - onStartIter) / 1000000

            val onStartTailRec = System.nanoTime
            val kNeighborsTailRec = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc).cache()
            kNeighborsTailRec.count()
            val onFinishTailRec = System.nanoTime
            val tailRecDuration = (onFinishTailRec - onStartTailRec) / 1000000

            val rNeighborsTailRec = NeighborsReverser.findReverseNeighbors(kNeighborsTailRec)
            val antihubTailRec = new Antihub().antihub(rNeighborsTailRec)
            val predictionsAndLabelsAntihubTailRec = classifications.join(antihubTailRec).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihubTailRec = new BinaryClassificationMetrics(predictionsAndLabelsAntihubTailRec)

            val rNeighborsIter = NeighborsReverser.findReverseNeighbors(kNeighborsIter)
            val antihubIter = new Antihub().antihub(rNeighborsIter)
            val predictionsAndLabelsAntihubIter = classifications.join(antihubIter).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihubIter = new BinaryClassificationMetrics(predictionsAndLabelsAntihubIter)

            println(s"k: $k, pivots: $pivotsAmount, seed: $seed, tailRecDuration: ${tailRecDuration}ms, tailRecAntihub: ${detectionMetricsAntihubTailRec.areaUnderROC()}, iterDuration: ${iterDuration}ms, iterAntihub: ${detectionMetricsAntihubIter.areaUnderROC()}")
            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }
}
