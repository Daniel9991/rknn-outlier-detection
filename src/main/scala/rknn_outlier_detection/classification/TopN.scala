package rknn_outlier_detection.classification
import org.apache.spark.rdd.RDD

class TopN(n: Integer) extends ClassificationStrategy {
    override def classify(instances: RDD[(String, Double)], normalLabel: String, outlierLabel: String): RDD[(String, String)] = {
        val sortedInstances = instances.sortBy(_._2, ascending = false)
        // TODO is this the most efficient way
        sortedInstances.zipWithIndex().map(tuple => {
            val (tuple2, index) = tuple
            val (id, _) = tuple2

            if(index < n) (id, outlierLabel) else (id, normalLabel)
        })
    }
}
