package rknn_outlier_detection.big_data.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

trait DetectionStrategy[A] {
    def detect(reverseNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)]
}
