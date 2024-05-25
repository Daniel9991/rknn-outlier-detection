package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

trait PivotSelector[A] {
    def findPivots(
                      instances: RDD[Instance[A]],
                      pivotsAmount: Int,
                      distanceFunction: DistanceFunction[A]
    ): Array[Instance[A]]
}
