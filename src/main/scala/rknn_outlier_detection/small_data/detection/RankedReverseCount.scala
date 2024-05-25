package rknn_outlier_detection.small_data.detection
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class RankedReverseCount[A] extends DetectionCriteria[A] {

    override def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[RNeighbor]]): Array[Double] = {
        val k = kNeighbors(0).length
        val rankDiscount = 0.7 / k.toDouble

        reverseNeighbors.map(rNeighborsBatch => {
            val rankedCount = rNeighborsBatch.map(n => 1 - n.rank * rankDiscount).sum
            normalizeReverseNeighborsCount(rankedCount)
        })
    }

    def normalizeReverseNeighborsCount(count: Double): Double = {
        if (count == 0)
            1.0
        else
            1.0 / count
    }
}
