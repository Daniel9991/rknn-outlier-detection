package rknn_outlier_detection.small_data.search.pivot_based

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

trait PivotSelector[A] {
    def findPivots(
                      instances: Array[Instance[A]],
                      distanceFunction: DistanceFunction[A],
                      pivotsAmount: Int
    ): Array[Instance[A]]
}
