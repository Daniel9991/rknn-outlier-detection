package rknn_outlier_detection.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}

trait KNNSearchStrategy {
    def findKNeighbors(instances: RDD[Instance], k: Integer, sc: SparkContext): RDD[(String, Array[KNeighbor])]
}
