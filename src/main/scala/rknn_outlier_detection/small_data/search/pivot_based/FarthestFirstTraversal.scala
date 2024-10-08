package rknn_outlier_detection.small_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.small_data.search.pivot_based.PivotSelector
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FarthestFirstTraversal(_objectSet: Array[Instance]) extends PivotSelector{

    val random = new Random()

    override def findPivots(
                               instances: Array[Instance],
                               distanceFunction: DistanceFunction,
                               pivotsAmount: Int
    ): Array[Instance] = {

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
}
