package rknn_outlier_detection.big_data.classification

import org.apache.spark.rdd.RDD

trait ClassificationStrategy {
    def classify(instances: RDD[(String, Double)], normalLabel: String, outlierLabel: String): RDD[(String, String)]
}

// Top N
// Top percentage
// Threshold