package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, monotonically_increasing_id}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import rknn_outlier_detection.lof.LOF
import rknn_outlier_detection.shared.utils.ReaderWriter

object LOFExperiment {

    def main(args: Array[String]): Unit = {

        val datetime = java.time.LocalDateTime.now()
        val dateString = formatLocalDateTime(datetime)
        val nodes = if (args.length > 0) args(0).toInt else 1
        val k = if (args.length > 1) args(1).toInt else 100
        val datasetSize = if (args.length > 2) args(2).toInt else -1
        val partitions = if (args.length > 3) args(3).toInt else 64

        try {
            val config = new SparkConf()
//            config.setMaster("spark://127.0.0.1:7077")
//            config.setMaster("local[*]")
//            config.set("spark.executor.memory", "8g")
//            config.set("spark.driver.memory", "4g")
//            config.set("spark.driver.maxResultSize", "1g")

            val spark = SparkSession
                .builder()
                .config(config)
                .appName("LOFExample")
                .getOrCreate()

            import spark.implicits._

            val fullPath = System.getProperty("user.dir")
            val relativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"$fullPath\\$relativePath"

            val df = spark.read.format("csv")
                .option("header", "false")
                .schema(creditCardSchema)
                .load(datasetPath)
                .cache();

            val withId = df.withColumn("id", monotonically_increasing_id())

            val assembler = new VectorAssembler()
                .setInputCols(Array("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29"))
                .setOutputCol("features")
            val data = assembler.transform(withId).repartition(partitions)

            val startTime = System.nanoTime()
            val result = new LOF()
                .setMinPts(k)
                .transform(data)
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
            result.count()
            val endTime = System.nanoTime()
            val duration = (endTime - startTime) / 1000000
//            val sortedRes = result.sort(desc("lof"))

            val classifications = withId.select("id", "class").as[(Long, Int)].rdd.map(t => (t._1, t._2.toDouble))
            val rddResult: RDD[(Long, Double)] = result.as[(Long, Double)].rdd.map(t => (t._1, t._2))
            println(classifications.join(rddResult).take(20).map(t => s"${t._2._1}: ${t._2._2}").mkString("Class: Lof\n", "\n", "\n"))
            println(s"The amount of outliers is ${classifications.filter(t => t._2 == 1.0).count()}")
            val predictionsAndLabels = classifications.join(rddResult).map(tuple => (tuple._2._2, tuple._2._1))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$nodes,${if (datasetSize == -1) "full" else s"$datasetSize"},$k,${detectionMetrics.areaUnderROC()},${detectionMetrics.areaUnderPR()},0,0,$duration,$dateString,$partitions"
            saveStatistics(line)
        }
        catch {
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                val error = s"\nERROR - $getFormattedLocalDateTime\n$e"
                logError(error)
            }
        }
    }

    def oldMain(args: Array[String]): Unit = {

        val datetime = java.time.LocalDateTime.now()
        val dateString = formatLocalDateTime(datetime)
        val nodes = if (args.length > 0) args(0).toInt else 1
        val k = if (args.length > 1) args(1).toInt else 10
        val datasetSize = if (args.length > 2) args(2).toInt else -1
        val partitions = if (args.length > 3) args(3).toInt else 80

        try {
            val config = new SparkConf()
            config.setMaster("local[*]")
            val spark = SparkSession
                .builder()
                .config(config)
                .appName("LOFExample")
                .getOrCreate()

            import spark.implicits._

            val fullPath = System.getProperty("user.dir")
            val datasetPath = s"${fullPath}\\testingDatasets\\creditcardMinMaxScaled.csv"

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex
                .map { case (line, index) => {
                    val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                    val classification = if (line.last == "1") "1.0" else "0.0"
                    (index.toInt, Vectors.dense(attributes), classification)
                }
            }


            val df = spark.createDataFrame(instancesAndClassification).toDF("id", "attributes", "class")

            val assembler = new VectorAssembler()
                .setInputCols(Array("attributes"))
                .setOutputCol("features")
            val data = assembler.transform(df).repartition(partitions)

            val startTime = System.nanoTime()
            val result = new LOF()
                .setMinPts(k)
                .transform(data)
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
            result.count()
            val endTime = System.nanoTime()
            val duration = (endTime - startTime) / 1000000
            val sortedRes = result.sort(desc("lof"))

            val classifications = instancesAndClassification.map(t => (t._1, t._3))
            val rddResult: RDD[(Int, Double)] = sortedRes.as[(Int, Double)].rdd.map(t => (t._1, t._2))
            val predictionsAndLabels = classifications.join(rddResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$nodes,${if (datasetSize == -1) "full" else s"$datasetSize"},$k,${detectionMetrics.areaUnderROC()},${detectionMetrics.areaUnderPR()},0,0,$duration,$dateString,$partitions"
            saveStatistics(line)
        }
        catch {
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                val error = s"\nERROR - $getFormattedLocalDateTime\n$e"
                logError(error)
            }
        }
    }

    def saveStatistics(line: String, file: String = ""): Unit = {
        val filename = if (file == "") s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\lof_experiments.csv" else file
        val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader = false).map(line => line.mkString(",")).mkString("\n")
        val updatedRecords = s"$previousRecordsText\n$line"
        ReaderWriter.writeToFile(filename, updatedRecords)
    }

    val creditCardSchema = new StructType(Array(
        new StructField("col1", DataTypes.DoubleType),
        new StructField("col2", DataTypes.DoubleType),
        new StructField("col3", DataTypes.DoubleType),
        new StructField("col4", DataTypes.DoubleType),
        new StructField("col5", DataTypes.DoubleType),
        new StructField("col6", DataTypes.DoubleType),
        new StructField("col7", DataTypes.DoubleType),
        new StructField("col8", DataTypes.DoubleType),
        new StructField("col9", DataTypes.DoubleType),
        new StructField("col10", DataTypes.DoubleType),
        new StructField("col11", DataTypes.DoubleType),
        new StructField("col12", DataTypes.DoubleType),
        new StructField("col13", DataTypes.DoubleType),
        new StructField("col14", DataTypes.DoubleType),
        new StructField("col15", DataTypes.DoubleType),
        new StructField("col16", DataTypes.DoubleType),
        new StructField("col17", DataTypes.DoubleType),
        new StructField("col18", DataTypes.DoubleType),
        new StructField("col19", DataTypes.DoubleType),
        new StructField("col20", DataTypes.DoubleType),
        new StructField("col21", DataTypes.DoubleType),
        new StructField("col22", DataTypes.DoubleType),
        new StructField("col23", DataTypes.DoubleType),
        new StructField("col24", DataTypes.DoubleType),
        new StructField("col25", DataTypes.DoubleType),
        new StructField("col26", DataTypes.DoubleType),
        new StructField("col27", DataTypes.DoubleType),
        new StructField("col28", DataTypes.DoubleType),
        new StructField("col29", DataTypes.DoubleType),
        new StructField("class", DataTypes.IntegerType),
    ))
}
