package rknn_outlier_detection.search
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.Main.sc
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.{DistanceFunctions, DistanceObject}

class LAESA extends KNNSearchStrategy{

    /**
     * ...
     *
     * @param instances Collection of instances to process
     * @param k Amount of neighbors for each instance
     * @param sc SparkContext of the running app
     * @return RDD containing a tuple for
     *         each instance with its array of neighbors
     */
    override def findKNeighbors(
       instances: RDD[Instance],
       k: Integer,
       sc: SparkContext
   ): RDD[(String, Array[KNeighbor])] = {

        // Select base pivots
        val basePivots = findBasePivots(instances)

        // Calculate and store the distance between a pivot and every instance, for all pivots
        val pivotsDistances = basePivots
            .cartesian(instances)
            .filter(tuple => tuple._1.id != tuple._2.id)
            .map(tuple => {
                val (pivot, instance) = tuple
                val distance = DistanceFunctions.euclidean(pivot.attributes, instance.attributes)
                val distanceObject = new DistanceObject(pivot.id, instance.id, distance)
            })




        sc.parallelize(Seq())
    }

    def findBasePivots(instances: RDD[Instance]): RDD[Instance] = {

        sc.parallelize(Seq())
    }
}
