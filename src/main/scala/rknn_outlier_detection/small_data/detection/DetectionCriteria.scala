package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

trait DetectionCriteria {
    def scoreInstancesFromInstances(instances: Array[Instance]): Array[Double]
    def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[RNeighbor]]): Array[Double]
}
