package rknn_outlier_detection

import org.apache.spark._
import rknn_outlier_detection.custom_objects.Instance
import rknn_outlier_detection.search.ExhaustiveSearch
import rknn_outlier_detection.utils.Utils.readCSV

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

        val FILE_PATH = "datasets/Iris-virginica_Iris-setosa_4.csv"
        val k = 21
        val topN = 6

//        Read data and convert to instances

        val data = readCSV(FILE_PATH, sc)

        val instances = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            new Instance(index.toString, line.slice(0, 4).map(_.toDouble), line.last)
        })

        println("hi, mom!")

        // TODO take reverse neighbors search out of ExhaustiveSearch
    }
}
