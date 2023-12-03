package rknn_outlier_detection

import org.apache.spark.ml.feature.{MaxAbsScaler, MinMaxScaler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import rknn_outlier_detection.utils.ReaderWriter
import rknn_outlier_detection.utils.Utils.readCSV
import org.apache.spark.ml.linalg.Vectors

object Preprocessing {

    def main(args: Array[String]): Unit = {

        val testingDatasetFolder = "testingDatasets"
        val fileName = "creditcardMinMaxScaled.csv"

        val spark:SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("myapp")
            .getOrCreate()

        val data = ReaderWriter.readCSV("datasets/creditcard.csv", hasHeader=true)
        val dfFuel = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            (index, Vectors.dense(line.slice(0, line.length - 1).map(_.toDouble)), line.last)
        }).toSeq

        val df = spark.createDataFrame(dfFuel).toDF("id", "features", "class")

        val scaler = new MinMaxScaler()
            .setInputCol("features")
            .setOutputCol("scaledFeatures")

        // Compute summary statistics and generate MaxAbsScalerModel
        val model = scaler.fit(df)

        // rescale each feature to range [0, 1]
        val scaledData = model.transform(df)

        val processedData = scaledData.select("scaledFeatures", "class").coalesce(1)
            .rdd.collect().map(row => {
                val stringifiedLine = row(0).toString
                val bracketLess = stringifiedLine.slice(1, stringifiedLine.length - 1)
                bracketLess + "," + row(1).toString
            }).mkString("\n")

        ReaderWriter.writeToFile(s"$testingDatasetFolder/$fileName", processedData)
    }
}
