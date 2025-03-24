package rknn_outlier_detection.small_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.small_data.search.pivot_based.PivotSelector
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FarthestFirstTraversal(){

    def findPivots(_objectSet: Array[Instance], distanceFunction: DistanceFunction, pivotsAmount: Int, random: Random = new Random()): Array[Instance] = {

        val objectSet = ArrayBuffer.from(_objectSet)
        val firstPivot = objectSet.remove(random.nextInt(objectSet.length))
        val pivots = new ArrayBuffer[Instance]()
        pivots.append(firstPivot)

        while(pivots.length < pivotsAmount){
            val nextPivot = objectSet.map(obj => {
                val distanceToPivots = pivots.map(pivot => distanceFunction(obj.data, pivot.data)).min
                (obj, distanceToPivots)
            }).maxBy(_._2)._1

            objectSet.remove(objectSet.indexOf(nextPivot))
            pivots.append(nextPivot)
        }

        pivots.toArray
    }

    def findTailRecursivePivots(objectSet: Array[Instance], distanceFunction: DistanceFunction, pivotsAmount: Int, random: Random = new Random()): Array[Instance] = {

        @tailrec
        def tailRecFFT(objectSet: Vector[Instance], pivots: ArrayBuffer[Instance]): Array[Instance] = {
            if(objectSet.length == pivotsAmount){
                pivots.toArray
            }
            else{
                val nextPivot = objectSet.map(obj => {
                    val distanceToPivots = pivots.map(pivot => distanceFunction(obj.data, pivot.data)).min
                    (obj, distanceToPivots)
                }).maxBy(_._2)._1
                tailRecFFT(objectSet.filter(o => o.id != nextPivot.id), pivots += nextPivot)
            }
        }

        val firstPivotIndex = random.nextInt(objectSet.length)
        val firstPivot = objectSet(firstPivotIndex)

        tailRecFFT(objectSet.filter(o => o.id != firstPivot.id).toVector, ArrayBuffer(firstPivot))
    }
}
