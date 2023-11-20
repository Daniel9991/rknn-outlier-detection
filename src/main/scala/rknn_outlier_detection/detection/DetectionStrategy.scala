package rknn_outlier_detection.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.Instance

trait DetectionStrategy {
    def detect(instances: RDD[Instance]): RDD[(String, Double)]
}
