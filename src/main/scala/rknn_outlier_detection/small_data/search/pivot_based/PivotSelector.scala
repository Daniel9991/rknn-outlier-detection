package rknn_outlier_detection.small_data.search.pivot_based

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

trait PivotSelector {
    def findPivots(
        instances: Array[Instance],
        distanceFunction: DistanceFunction,
        pivotsAmount: Int
    ): Array[Instance]
}
