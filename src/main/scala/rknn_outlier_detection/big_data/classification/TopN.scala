package rknn_outlier_detection.big_data.classification
import org.apache.spark.rdd.RDD

class TopN(n: Integer) extends ClassificationStrategy {
    override def classify(scores: RDD[(Int, Double)], normalLabel: String, outlierLabel: String): RDD[(Int, Double, String)] = {
        val sortedInstances = scores.sortBy(_._2, ascending = false)
        sortedInstances.zipWithIndex().map{case ((id, score), index) =>
            (id, score, if(index < n) outlierLabel else normalLabel)
        }
    }
}
