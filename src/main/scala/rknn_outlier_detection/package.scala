import rknn_outlier_detection.shared.custom_objects.Instance
import rknn_outlier_detection.shared.distance.DistanceFunctions

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

package object rknn_outlier_detection {

    type Pivot = Instance
    type PivotWithCountAndDist = (Instance, Int, Double)
    type PivotWithBigCountAndDist = (Instance, BigInt, Double)
    type PivotWithCount = (Instance, Int)
    type PivotWithBigCount = (Instance, BigInt)

    type DistanceFunction = (Array[Double], Array[Double]) => Double

    val euclidean: DistanceFunction = DistanceFunctions.euclidean

    val manhattan: DistanceFunction = DistanceFunctions.manhattan

    def time[T](block: => T): T = {
        val before = System.nanoTime
        val result = block
        val after = System.nanoTime
        println(s"Elapsed time: ${(after - before) / 10000000}ms")
        result
    }

    def selectMinimumClosestPivotsRec(instance: Instance, k: Int, pivots: Array[PivotWithCountAndDist]): Array[(Instance, Instance)] = {
        @tailrec
        def minimumClosestPivotsTailRec(instance: Instance, k: Int, remainingPivots: Array[PivotWithCountAndDist], selectedPivots: ArrayBuffer[PivotWithCount]): Array[(Instance, Instance)] = {
            if(remainingPivots.isEmpty || (selectedPivots.nonEmpty && selectedPivots.map{case (_, count) => count}.sum > k)){
                selectedPivots.toArray.map{case (pivot, _) => (pivot, instance)}
            }
            else{
                val closestPivot = remainingPivots.minBy{case (_, _, distanceToPivot) => distanceToPivot}
                val formatted: PivotWithCount = (closestPivot._1, closestPivot._2)
                selectedPivots += formatted

                val updatedRemaining = remainingPivots.filter(p => p._1.id != closestPivot._1.id)
                minimumClosestPivotsTailRec(instance, k, updatedRemaining, selectedPivots)
            }
        }

        minimumClosestPivotsTailRec(instance, k, pivots, ArrayBuffer.empty[PivotWithCount])
    }
}
