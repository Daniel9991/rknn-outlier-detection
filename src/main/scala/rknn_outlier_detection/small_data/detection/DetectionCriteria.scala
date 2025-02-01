package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

trait DetectionCriteria extends Serializable{
    def scoreInstances(reverseNeighbors: Array[(Int, Array[RNeighbor])]): Array[(Int, Double)]
}
