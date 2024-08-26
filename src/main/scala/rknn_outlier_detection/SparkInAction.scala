package rknn_outlier_detection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SparkSession}
import rknn_outlier_detection.shared.custom_objects.Instance

object SparkInAction {

    def main(args: Array[String]): Unit ={

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"

        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled.csv"

        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val before = System.nanoTime

        val spark = SparkSession.builder()
            .appName("ProcessingDatasets")
            .master("local[*]")
            .getOrCreate();

        val df = spark.read.format("csv")
            .option("header", "false")
            .load(datasetPath);

        df.toJavaRDD.zipWithIndex().map(tuple => {
            val (row, index) = tuple
//            new Instance(index.toString, row.)
        })

        df.show(5)
        df.printSchema()
    }
}
