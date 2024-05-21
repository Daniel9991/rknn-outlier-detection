package rknn_outlier_detection.big_data.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

object RankedReverseCount extends DetectionStrategy {
    private def normalizeReverseNeighborsCount(count: Double): Double = {
        if(count == 0.0)
            1.0
        else
            1.0 / count
    }

    def antihubFromInstances(instances: RDD[Instance]): RDD[(String, Double)] ={
        instances.map(instance => (instance.id, normalizeReverseNeighborsCount(instance.rNeighbors.length)))
    }

    def calculateAnomalyDegree(idsWithRNeighbors: RDD[(String, Array[RNeighbor])], k: Int): RDD[(String, Double)] ={
        val rankDiscount = 0.7 / k.toDouble
        idsWithRNeighbors.map(tuple => {
            val (id, reverseNeighbors) = tuple
            val rankedCount = reverseNeighbors.map(n => 1 - n.rank * rankDiscount).sum
            (id, normalizeReverseNeighborsCount(rankedCount))
        })
    }

    override def detect(instances: RDD[Instance]): RDD[(String, Double)] = {
        antihubFromInstances(instances)
    }
}
