package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class Antihub extends DetectionCriteria{
    override def scoreInstances(reverseNeighbors: Array[(Int, Array[RNeighbor])]): Array[(Int, Double)] = {
        reverseNeighbors.map{
            case (id, rNeighbors) => (id, Antihub.normalizeReverseNeighborsCount(rNeighbors.length))
        }
    }
}

object Antihub {

    def normalizeReverseNeighborsCount(count: Int): Double = {
        if (count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    def normalizeReverseNeighborsCount(count: Double): Double = {
        if (count == 0.0)
            1.0
        else
            1.0 / count
    }
}
