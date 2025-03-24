package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.Instance

trait PivotSelectionStrategy {
    def selectPivots(instances: RDD[Instance], pivotsAmount: Int): Array[Instance]
}
