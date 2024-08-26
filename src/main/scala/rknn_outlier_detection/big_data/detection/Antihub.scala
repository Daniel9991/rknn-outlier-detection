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

    def antihub(idsWithRNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    override def detect(reverseNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] = {
        antihub(reverseNeighbors)
    }
}
