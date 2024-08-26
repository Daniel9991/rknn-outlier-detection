package rknn_outlier_detection

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.classification.ClassificationStrategy
import rknn_outlier_detection.shared.custom_objects.Instance
import rknn_outlier_detection.big_data.detection.DetectionStrategy
import rknn_outlier_detection.big_data.search.reverse_knn.ReverseNeighborsSearch
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.shared.distance.DistanceFunctions

class Detector(
  val searchStrategy: KNNSearchStrategy,
  val detectionStrategy: DetectionStrategy,
  val classificationStrategy: ClassificationStrategy,
  val normalLabel: String,
  val outlierLabel: String,
  val sc: SparkContext
) {

    def detectOutliers(instances: RDD[Instance], k: Int, distanceFunction: DistanceFunction): RDD[(String, String)] ={

        // Find kNeighbors
        val x = searchStrategy.findKNeighbors(instances, k, distanceFunction, sc)

        // Find reverse neighbors
        val y = ReverseNeighborsSearch.findReverseNeighbors(x)

        val outlierScores = detectionStrategy.detect(y)
        classificationStrategy.classify(outlierScores, normalLabel, outlierLabel)
    }
}
