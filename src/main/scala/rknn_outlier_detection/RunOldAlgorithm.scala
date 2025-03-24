package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.functions.{concat, concat_ws, lit}
import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import rknn_outlier_detection.BigDataExperiment.saveStatistics
import rknn_outlier_detection.big_data.alternative_methods.SameThingByPartition
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.detection
import rknn_outlier_detection.small_data.detection.DetectionStrategy
//import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import rknn_outlier_detection.knnw_bigdata.{Clasificacion, KNNW_BigData, Tupla, TuplaFase1, TuplaFase2}

import scala.concurrent.duration.{Duration, NANOSECONDS}

object RunOldAlgorithm {
    def main(args: Array[String]): Unit = {
        knnw_big_data_experiments(args)
    }

    class CustomListener extends SparkListener {
        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//            stageCompleted.stageInfo.submissionTime
            println(s"-- - - - -- - - - -- Stage completed, runTime: ${stageCompleted.stageInfo.taskMetrics.executorRunTime}")
        }

//        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//            println(taskEnd.taskInfo)
//            println(taskEnd.taskMetrics)
//        }
    }

//    0.9411048148959573, 0.21936084328043814, 1482009
    def knnw_big_data_experiments(args: Array[String]): Unit = {
        val k = if(args.length > 0) args(0).toInt else 10
        val p = if(args.length > 1) args(1).toDouble else 0.1
        val partitions = if(args.length > 2) args(2).toInt else 320

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf()
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[Tupla], classOf[Clasificacion], classOf[TuplaFase1], classOf[TuplaFase2]))
            config.setMaster("local[*]")

            val spark = SparkSession.builder()
                .config(config)
                .appName(s"Test k: $k p: $p ${partitions}")
                .getOrCreate();

//            val myListener = new CustomListener
//            spark.sparkContext.addSparkListener(myListener)

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.persist()

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            val data = spark.createDataset(instances.repartition(partitions).map(instance => Tupla(instance.id.toString, instance.data))).persist()
            val onStart = System.nanoTime()
            val outlierDegrees = new KNNW_BigData(k, p).train(data, spark).map(res => (res.ID.toInt, res.ia)).persist()
            outlierDegrees.count()
            val onFinish = System.nanoTime
            val duration = (onFinish - onStart) / 1000000
            val predictionsAndLabels = classifications.join(outlierDegrees.rdd).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$k,$p,$partitions,${detectionMetrics.areaUnderROC()},${detectionMetrics.areaUnderPR()},$duration"
            saveStatistics(line)

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def saveStatistics(line: String, file: String = ""): Unit = {
        val filename = if(file == "") s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\knnw_big_data_results.csv" else file
        val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
        val updatedRecords = s"$previousRecordsText\n$line"
        ReaderWriter.writeToFile(filename, updatedRecords)
    }
}