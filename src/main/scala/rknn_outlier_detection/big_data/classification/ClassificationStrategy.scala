package rknn_outlier_detection.big_data.classification

import org.apache.spark.rdd.RDD

trait ClassificationStrategy {
    def classify(instances: RDD[(Int, Double)], normalLabel: String, outlierLabel: String): RDD[(Int, String)]
}

// Top N
// Top percentage
// Threshold