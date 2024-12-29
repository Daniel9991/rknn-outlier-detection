package rknn_outlier_detection.big_data.classification

import org.apache.spark.rdd.RDD

class Threshold(threshold: Double) extends ClassificationStrategy {

    if(threshold > 1.0 || threshold < 0.0) throw new Exception("Threshold value must be a decimal number between 0 and 1")

    override def classify(instances: RDD[(Int, Double)], normalLabel: String, outlierLabel: String): RDD[(Int, String)] = {
        instances.map(tuple => {
            val (id, anomalyDegree) = tuple

            (id, if(anomalyDegree >= threshold) normalLabel else outlierLabel)
        })
    }
}
