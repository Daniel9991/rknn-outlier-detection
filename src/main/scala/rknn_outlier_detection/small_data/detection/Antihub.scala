package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class Antihub extends DetectionCriteria {

    override def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[RNeighbor]]): Array[Double] = {
        reverseNeighbors.map(
            rNeighborsBatch => Antihub.normalizeReverseNeighborsCount(rNeighborsBatch.length)
        )
    }
}

object Antihub{
    def normalizeReverseNeighborsCount(count: Int): Double = {
        if (count == 0)
            1.0
        else
            1.0 / count.toDouble
    }
}
