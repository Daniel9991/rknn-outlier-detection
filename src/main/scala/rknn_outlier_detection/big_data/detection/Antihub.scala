package rknn_outlier_detection.big_data.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, Neighbor}

object Antihub extends DetectionStrategy {

    private def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    def antihubFromInstances(instances: RDD[Instance]): RDD[(String, Double)] ={
        instances.map(instance => (instance.id, normalizeReverseNeighborsCount(instance.rNeighbors.length)))
    }

    def antihub(idsWithRNeighbors: RDD[(String, Array[Neighbor])]): RDD[(String, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    override def detect(instances: RDD[Instance]): RDD[(String, Double)] = {
        antihubFromInstances(instances)
    }
}
