package rknn_outlier_detection.small_data.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}

trait KNNSearchStrategy {
    def findKNeighbors(instances: Array[Instance], k: Int, distanceFunction: DistanceFunction): Array[Array[KNeighbor]]
}
