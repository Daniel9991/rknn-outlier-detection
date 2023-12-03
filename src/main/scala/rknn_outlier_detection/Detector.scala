package rknn_outlier_detection

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.classification.ClassificationStrategy
import rknn_outlier_detection.custom_objects.Instance
import rknn_outlier_detection.detection.DetectionStrategy
import rknn_outlier_detection.search.{ExhaustiveSearch, KNNSearchStrategy, ReverseKNNSearch}

class Detector(
                  val searchStrategy: KNNSearchStrategy,
                  val detectionStrategy: DetectionStrategy,
                  val classificationStrategy: ClassificationStrategy,
                  val normalLabel: String,
                  val outlierLabel: String,
                    val sc: SparkContext
) {

    def detectOutliers(instances: RDD[Instance], k: Integer): RDD[(String, String)] ={

        // Find kNeighbors

        val x = searchStrategy.findKNeighbors(instances, k, sc)

        // Find reverse neighbors

        val y = ReverseKNNSearch.findReverseNeighbors(x)

        // TODO analyze the need for this, as it adds extra complexity
        // Get k neighbors and reverse neighbors into instances
        val equippedInstances = instances.map(instance => (instance.id, instance))
            .join(x)
            .map(tuple => {
                val (key, nestedTuple) = tuple
                val (instance, kNeighbors) = nestedTuple
                instance.kNeighbors = kNeighbors
                (key, instance)
            })
            .join(y)
            .map(tuple => {
                val (key, nestedTuple) = tuple
                val (instance, rNeighbors) = nestedTuple
                instance.rNeighbors = rNeighbors
                instance
            })

        val outlierScores = detectionStrategy.detect(equippedInstances)

        classificationStrategy.classify(outlierScores, normalLabel, outlierLabel)
    }
}
