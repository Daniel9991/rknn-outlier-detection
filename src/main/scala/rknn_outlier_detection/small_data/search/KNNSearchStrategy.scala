package rknn_outlier_detection.small_data.search

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}

trait KNNSearchStrategy {
    def findKNeighbors(instances: Array[Instance], k: Int, distanceFunction: DistanceFunction): Array[(Int, Array[KNeighbor])]
}
