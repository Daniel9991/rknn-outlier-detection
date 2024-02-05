package rknn_outlier_detection

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import rknn_outlier_detection.custom_objects.Instance
import rknn_outlier_detection.search.ExhaustiveSearch
import rknn_outlier_detection.utils.Utils.readCSV

object Main {

//    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

        val spark:SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("myapp")
            .getOrCreate()

        println("Hi, mom!")
    }
}
