package rknn_outlier_detection.small_data.search.pivot_based

import rknn_outlier_detection.shared.custom_objects.Instance

trait PivotSelector {
    def findPivots(
        instances: Array[Instance],
        distanceFunction: (Array[Double], Array[Double]) => Double,
        pivotsAmount: Int
    ): Array[Instance]
}
