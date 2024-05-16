package rknn_outlier_detection.small_data.detection
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, Neighbor}

object RankedReverseCount extends DetectionCriteria {
    override def scoreInstancesFromInstances(instances: Array[Instance]): Array[Double] = {
        scoreInstances(instances.map(_.kNeighbors), instances.map(_.rNeighbors))
    }

    override def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[Neighbor]]): Array[Double] = {
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
