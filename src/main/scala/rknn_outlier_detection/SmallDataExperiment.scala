package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.{SmallDataDetector, detection}

object SmallDataExperiment {
    def main(args: Array[String]): Unit = {
        val seeds = Array("431", "90783", "34634", "56342")
        val ks = Array("1", "5", "10", "25", "50", "100", "200", "400", "600", "800", "1000")
        seeds.foreach(seed => {
            ks.foreach(k => smallDataExperiment(Array(k, seed, "antihub")))
        })
    }

    def smallDataExperiment(args: Array[String]): Unit = {

        val pivotsAmount = 142
        val distanceFunction = euclidean
        val k = if (args.length > 0) args(0).toInt else 200
        val seed = if (args.length > 1) args(1).toInt else 87654
        val detectionMethod = if (args.length > 2) args(2) else "antihub"

        try {
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val detectionCriteria: detection.DetectionStrategy = detectionMethod match {
                case "antihub" => new detection.Antihub()
                case "ranked" => new detection.RankedReverseCount(k, 0.7)
                case "refined" => new detection.AntihubRefined(0.1, 0.3)
            }

            val rawData = ReaderWriter.readCSV(datasetPath, hasHeader = false)
            val instancesAndClassification = rawData.zipWithIndex.map { case (line, index) =>
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index, attributes), classification)
            }.toSeq

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map { case (instance, classification) => (instance.id, classification) }

            val onStart = System.nanoTime()
            val outlierDegrees = new SmallDataDetector().detect(instances, k, pivotsAmount, seed, detectionCriteria, distanceFunction)
            val onFinish = System.nanoTime
            val duration = (onFinish - onStart) / 1000000

            val config = new SparkConf()
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.setMaster("local[*]")

            val spark = SparkSession.builder()
                .config(config)
                .appName(s"Small Data Test k: $k seed: $seed method: $detectionMethod")
                .getOrCreate();

            val sc = spark.sparkContext

            val predictionsAndLabels = sc.parallelize(classifications).join(sc.parallelize(outlierDegrees)).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$k,$seed,$detectionMethod,${detectionMetrics.areaUnderROC()},$duration"
            saveStatistics(line)

            println(s"---------------Done executing-------------------")
        }
        catch {
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def saveStatistics(line: String, file: String = ""): Unit = {
        val filename = if(file == "") s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\small_data_experiments.csv" else file
        val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
        val updatedRecords = s"$previousRecordsText\n$line"
        ReaderWriter.writeToFile(filename, updatedRecords)
    }
}
