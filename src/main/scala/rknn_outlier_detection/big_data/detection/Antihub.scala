package rknn_outlier_detection.big_data.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}

class Antihub[A] extends DetectionStrategy[A] with Serializable {

    private def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    def antihubFromInstances(instances: RDD[Instance[A]]): RDD[(String, Double)] ={
        instances.map(instance => (instance.id, normalizeReverseNeighborsCount(instance.rNeighbors.length)))
    }

    def antihub(idsWithRNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    override def detectFromInstances(instances: RDD[Instance[A]]): RDD[(String, Double)] = {
        antihubFromInstances(instances)
    }

    override def detect(reverseNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] = {
        antihub(reverseNeighbors)
    }
}
