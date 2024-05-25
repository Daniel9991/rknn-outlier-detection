package rknn_outlier_detection.small_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.big_data.search.pivot_based.PivotSelector
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FarthestFirstTraversal[A](_objectSet: Array[Instance[A]]) extends PivotSelector[A]{

    val random = new Random(345)

    override def findPivots(
                               instances: RDD[Instance[A]],
                               pivotsAmount: Int,
                               distanceFunction: DistanceFunction[A]
    ): Array[Instance[A]] = {

        val objectSet = ArrayBuffer.from(_objectSet)
        val firstPivot = objectSet.remove(random.nextInt(objectSet.length))
        val pivots = new ArrayBuffer[Instance[A]]()
        pivots.append(firstPivot)

        while(pivots.length < pivotsAmount){
            val nextPivot = objectSet.map(obj => {
                val distanceToPivots = pivots.map(pivot => distanceFunction(obj.attributes, pivot.attributes)).min
                (obj, distanceToPivots)
            }).maxBy(_._2)._1

            objectSet.remove(objectSet.indexOf(nextPivot))
            pivots.append(nextPivot)
        }

        pivots.toArray
    }
}
