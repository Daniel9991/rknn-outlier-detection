package rknn_outlier_detection.detection

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, Neighbor}

object Techniques {

    def antihubFromInstances(instances: RDD[Instance]): RDD[(String, Double)] ={
        instances.map(instance => (instance.id, normalizeReverseNeighborsCount(instance.rNeighbors.length)))
    }

    def antihub(idsWithRNeighbors: RDD[(String, Array[Neighbor])]): RDD[(String, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }
}
