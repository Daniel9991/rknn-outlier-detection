package rknn_outlier_detection.big_data.classification
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class StandardDeviation(val spark: SparkSession) extends ClassificationStrategy {
    override def classify(scores: RDD[(Int, Double)], normalLabel: String, outlierLabel: String): RDD[(Int, Double, String)] = {

        val mean = scores.map(_._2).reduce(_+_) / scores.count()
        val standardDeviation = scala.math.sqrt(scores.map{case (id, score) => scala.math.pow(score - mean, 2)}.reduce(_+_) / scores.count().toDouble)
        scores.map{case (id, score) => (id, score, if(score > mean + 3 * standardDeviation) outlierLabel else normalLabel)}
    }
}
