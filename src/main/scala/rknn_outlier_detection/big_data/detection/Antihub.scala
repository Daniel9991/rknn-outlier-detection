package rknn_outlier_detection.big_data.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

class Antihub extends DetectionStrategy with Serializable {

    private def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }


    private def originalNormalizationFunction(value: Double): Double = {
        1 / (1 + value)
    }

    def antihub(idsWithRNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    def antihubOriginal(idsWithRNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, originalNormalizationFunction(tuple._2.length)))
    }

    override def detect(reverseNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] = {
        antihub(reverseNeighbors)
    }
}
