package rknn_outlier_detection.shared.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Utils {

    def readCSV(filePath: String, sc: SparkContext): RDD[Array[String]] = {
        val lines = sc.textFile(filePath)
        lines.map(line => line.split(",").map(_.trim))
    }

    def countTokensInCSV(tokens: RDD[Array[String]]): Int = {
        val lineSizes = tokens.map(_.length)
        val sum = lineSizes.sum()
        sum.toInt
    }

    def getRandomElement[A](seq: Seq[A], random: Random): A =
        seq(random.nextInt(seq.length))

    def arrayEquals[T](a1: Array[T], a2: Array[T], size: Int): Boolean = {
        var i = 0
        while (i < size) {
            if (a1(i) != a2(i)) return false
            i += 1
        }
        true
    }

    def sortNeighbors(n1: KNeighbor, n2: KNeighbor): Boolean = {
        n1.distance < n2.distance
    }

    def insertNeighborInArray(neighbors: Array[KNeighbor], neighbor: KNeighbor): Array[KNeighbor] = {

        var newNeighborIndex = neighbors.length - 1

        neighbors(newNeighborIndex) = neighbor

        while (neighbors(0) != neighbor && (neighbors(newNeighborIndex - 1) == null || neighbor.distance < neighbors(newNeighborIndex - 1).distance)) {
            neighbors(newNeighborIndex) = neighbors(newNeighborIndex - 1)
            newNeighborIndex -= 1
            neighbors(newNeighborIndex) = neighbor
        }

        neighbors
    }

    /**
     * Add a new KNeighbor to an array.
     * If the array contains empty spots i.e. there is a position that
     * contains null, find the index of the position and insert the neighbor there.
     * If the array is full, insert neighbor in the last position.
     *
     * After insertion, have new neighbor work its way down from the insertion
     * position as necessary comparing itself with the neighbor below (or ot its left).
     *
     * @param kNeighbors  Array of KNeighbors that can contain null spots. Expected to be sorted
     * @param newNeighbor KNeighbor to insert in array
     * @return Unit - The array is modified in place
     */
    def addNewNeighbor(
                          kNeighbors: Array[KNeighbor],
                          newNeighbor: KNeighbor
                      ): Unit = {

        var currentIndex: Int = 0

        // If array contains null, insert in first null position
        if (kNeighbors.contains(null)) {
            currentIndex = kNeighbors.indexOf(null)
            kNeighbors(currentIndex) = newNeighbor
        }
        // If array is full, insert in last position
        else {
            currentIndex = kNeighbors.length - 1
            kNeighbors(currentIndex) = newNeighbor
        }

        while (
            newNeighbor != kNeighbors.head &&
                newNeighbor.distance < kNeighbors(currentIndex - 1).distance
        ) {
            kNeighbors(currentIndex) = kNeighbors(currentIndex - 1)
            currentIndex -= 1
            kNeighbors(currentIndex) = newNeighbor
        }
    }

    def addCandidateSupportPivot(
                          candidatePivots: ArrayBuffer[(Instance, Double, Int, Int)],
                          newPivot: (Instance, Double, Int, Int)
                      ): ArrayBuffer[(Instance, Double, Int, Int)] = {


        candidatePivots.addOne(newPivot)
        var currentIndex: Int = candidatePivots.length - 1

        while (
            newPivot._1.id != candidatePivots.head._1.id &&
                newPivot._2 < candidatePivots(currentIndex - 1)._2
        ) {
            candidatePivots(currentIndex) = candidatePivots(currentIndex - 1)
            currentIndex -= 1
            candidatePivots(currentIndex) = newPivot
        }

        candidatePivots
    }

    def getKeyFromInstancesIds(id1: String, id2: String): String = {
        if(id1 < id2) s"${id1}_${id2}" else s"${id2}_${id1}"
    }
}
