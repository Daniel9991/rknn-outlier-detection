package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class Antihub[A] extends DetectionCriteria[A] {

    def normalizeReverseNeighborsCount(count: Int): Double = {
        if (count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    override def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[RNeighbor]]): Array[Double] = {
        reverseNeighbors.map(
            rNeighborsBatch => normalizeReverseNeighborsCount(rNeighborsBatch.length)
        )
    }
}
