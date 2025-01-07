package rknn_outlier_detection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import rknn_outlier_detection.big_data.alternative_methods.SameThingByPartition
import rknn_outlier_detection.shared.custom_objects.Instance

object SameThingByPartitionTest {
    def main(args: Array[String]): Unit = {
        val datasetSize = 50000
        val k = 800
        val seed = 56342
        val pivotsAmount = 25

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
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)
            val repartitioned = instances.repartition(16)
            val antihub = SameThingByPartition.detectAnomalies(repartitioned, pivotsAmount, seed, k, euclidean, sc)
            antihub.cache()
            antihub.count()

            val onFinish = System.nanoTime

            val duration = (onFinish - onStart) / 1000000

            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))

            val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)

            println(s"---------------Done executing-------------------")
            println(s"Result was: roc -> ${detectionMetricsAntihub.areaUnderROC()} taking ${duration}ms")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }
}
