package rknn_outlier_detection.big_data.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

class RankedReverseCount(val weight: Double, val k: Int) extends DetectionStrategy with Serializable {
    private def normalizeReverseNeighborsCount(count: Double): Double = {
        if(count == 0.0)
            1.0
        else
            1.0 / count
    }

    def calculateAnomalyDegree(idsWithRNeighbors: RDD[(Int, Array[RNeighbor])], k: Int): RDD[(Int, Double)] ={
        val rankDiscount = weight / k.toDouble
        idsWithRNeighbors.map(tuple => {
            val (id, reverseNeighbors) = tuple
            val rankedCount = reverseNeighbors.map(n => 1 - n.rank * rankDiscount).sum
            (id, normalizeReverseNeighborsCount(rankedCount))
        })
    }

    override def detect(reverseNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] = {
        calculateAnomalyDegree(reverseNeighbors, k)
    }
}
