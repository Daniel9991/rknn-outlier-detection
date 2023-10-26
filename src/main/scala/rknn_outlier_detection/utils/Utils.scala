package rknn_outlier_detection.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.KNeighbor

object Utils {

    def readCSV(filePath: String, sc: SparkContext): RDD[Array[String]] ={
        val lines = sc.textFile(filePath)
        lines.map(line => line.split(",").map(_.trim))
    }

    def countTokensInCSV(tokens: RDD[Array[String]]): Int={
        val lineSizes = tokens.map(_.length)
        val sum = lineSizes.sum()
        sum.toInt
    }

    def arrayEquals[T](a1: Array[T], a2: Array[T], size: Int): Boolean = {
        var i = 0
        while (i < size) {
            if (a1(i) != a2(i)) return false
            i += 1
        }
        true
    }

    def sortNeighbors(n1: KNeighbor, n2: KNeighbor): Boolean ={
        n1.distance < n2.distance
    }

    def insertNeighborInArray(neighbors: Array[KNeighbor], neighbor: KNeighbor): Array[KNeighbor] ={

        var newNeighborIndex = neighbors.length - 1

        neighbors(newNeighborIndex) = neighbor

        while(neighbors(0) != neighbor && (neighbors(newNeighborIndex - 1) == null || neighbor.distance < neighbors(newNeighborIndex - 1).distance)){
            neighbors(newNeighborIndex) = neighbors(newNeighborIndex - 1)
            newNeighborIndex -= 1
            neighbors(newNeighborIndex) = neighbor
        }

        neighbors
    }
}
