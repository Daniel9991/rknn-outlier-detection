package rknn_outlier_detection.big_data.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.Instance

trait DetectionStrategy {
    def detect(instances: RDD[Instance]): RDD[(String, Double)]
}
