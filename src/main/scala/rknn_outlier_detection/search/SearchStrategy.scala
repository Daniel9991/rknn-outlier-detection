package rknn_outlier_detection.search

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}

trait SearchStrategy {
    def findKNeighbors(instances: RDD[Instance], k: Integer): RDD[(String, Array[KNeighbor])]
    def findReverseNeighbors(instances: RDD[(String, Array[KNeighbor])]): RDD[(String, Array[Neighbor])]
}
