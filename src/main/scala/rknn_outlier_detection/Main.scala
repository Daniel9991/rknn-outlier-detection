package rknn_outlier_detection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.{ReaderWriter, Utils}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object Main {

    def main(args: Array[String]): Unit = {

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "results\\structured-results.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\new_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class DataFrameColNames(
            val datasetSize: String,
            val k: String,
            val pivotsAmount: String,
            val searchMethod: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val prc: String,
            val searchDuration: String,
            val reverseDuration: String,
            val detectionDuration: String,
            val totalDuration: String
        )

        val dfCols = new DataFrameColNames(
            datasetSize="DATASET_SIZE",
            k="K",
            pivotsAmount="PIVOTS_AMOUNT",
            searchMethod="SEARCH_METHOD",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            prc="PRC",
            searchDuration="SEARCH_DURATION",
            reverseDuration="REVERSE_DURATION",
            detectionDuration="DETECTION_DURATION",
            totalDuration="TOTAL_DURATION"
        )

        val datasetSchema = StructType(Array(
            StructField(dfCols.datasetSize, StringType, nullable = false),
            StructField(dfCols.k, IntegerType, nullable = false),
            StructField(dfCols.pivotsAmount, IntegerType, nullable = false),
            StructField(dfCols.searchMethod, StringType, nullable = false),
            StructField(dfCols.seed, IntegerType, nullable = false),
            StructField(dfCols.detectionMethod, StringType, nullable = false),
            StructField(dfCols.roc, DoubleType, nullable = false),
            StructField(dfCols.prc, DoubleType, nullable = false),
            StructField(dfCols.searchDuration, LongType, nullable = false),
            StructField(dfCols.reverseDuration, LongType, nullable = false),
            StructField(dfCols.detectionDuration, LongType, nullable = false),
            StructField(dfCols.totalDuration, LongType, nullable = false),
        ))

        val df = spark.read.format("csv")
            .option("header", "true")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        df.filter(col(dfCols.roc) > 0.9).orderBy(desc(dfCols.roc)).show(20)

        return

        val detectionMethods = Array("antihub", "ranked", "refined")
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800, 1000)
        val pivotsAmounts = Array(12, 25, 50, 100)
        val contents = new ArrayBuffer[String]()

        // Line plots of rocs for each pivots amount
        for(pivots <- pivotsAmounts){
            contents += s"${pivots} pivots"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                        && col(dfCols.k) === k
                        && col(dfCols.detectionMethod) === method
                        && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"

        // Line plots of rocs varying pivotsAmount for each detection technique
        for(detectionMethod <- detectionMethods){
            contents += s"roc by pivotsAmount ($detectionMethod)"
            contents += s"k,${pivotsAmounts.mkString(",")}"
            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(pivots <- pivotsAmounts){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === detectionMethod
                            && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"

        // Line plots of time(duration) varying pivotsAmount for each detection technique
        for(detectionMethod <- detectionMethods){
            contents += s"total_duration by pivotsAmount ($detectionMethod)"
            contents += s"k,${pivotsAmounts.mkString(",")}"
            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(pivots <- pivotsAmounts){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === detectionMethod
                            && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageDuration(values.select(dfCols.totalDuration), dfCols.totalDuration)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"


        //Line plot of time(duration) of search phase
        contents += s"search duration by pivots"
        contents += s"k,${pivotsAmounts.mkString(",")}"
        for(k <- kValues){
            val averages = new ArrayBuffer[Double]()
            for(pivots <- pivotsAmounts){
                val values = df.filter(
                    col(dfCols.pivotsAmount) === pivots
                        && col(dfCols.k) === k
                        && col(dfCols.searchMethod) === "classic"
                )

                val average = findAverageDuration(values.select(dfCols.searchDuration), dfCols.searchDuration)
                averages += average
            }
            contents += s"$k,${averages.mkString(",")}"
        }

        contents += "\n\n\n"

        // Line plot of rocs for exhaustive search
        contents += "exhaustive"
        contents += s"k,${detectionMethods.mkString(",")}"
        for(k <- kValues){
            val averages = new ArrayBuffer[Double]()
            for(method <- detectionMethods){
                val values = df.filter(
                    col(dfCols.k) === k
                    && col(dfCols.detectionMethod) === method
                    && col(dfCols.searchMethod) === "exhaustive"
                )

                val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                averages += average
            }
            contents += s"$k,${averages.mkString(",")}"
        }

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }

    def findAverageRoc(df: DataFrame, rocColName: String): Double = {
        val length = df.count()
        df.select(rocColName).rdd.map(r => r.getDouble(0)).reduce(_+_) / length.toDouble
    }

    def findAverageDuration(df: DataFrame, rocColName: String): Double = {
        val length = df.count()
        df.select(rocColName).rdd.map(r => r.getLong(0) / 1000).reduce(_+_).toDouble / length.toDouble
    }
}
