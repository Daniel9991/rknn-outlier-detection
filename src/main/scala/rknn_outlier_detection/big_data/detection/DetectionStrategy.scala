package rknn_outlier_detection.big_data.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

trait DetectionStrategy {
    def detect(reverseNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)]
}
