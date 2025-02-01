package rknn_outlier_detection.small_data.detection
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class RankedReverseCount(k: Int, weight: Double) extends DetectionCriteria {

    override def scoreInstances(reverseNeighbors: Array[(Int, Array[RNeighbor])]): Array[(Int, Double)] = {
        val rankDiscount = weight / k.toDouble

        reverseNeighbors.map{
            case (id, rNeighbors) => {
                val rankedCount = rNeighbors.map(n => 1 - n.rank * rankDiscount).sum
                (id, Antihub.normalizeReverseNeighborsCount(rankedCount))
            }
        }
    }
}
