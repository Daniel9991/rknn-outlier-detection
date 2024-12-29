package rknn_outlier_detection.big_data.search.reverse_knn

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.{KNeighbor, RNeighbor}

object NeighborsReverser {
    /**
     * Map instances to tuples of (kNeighborId, instanceId) for each kNeighbor
     * that an instance has.
     * Group pairs by key obtaining pairs of instanceId and iterable
     * of its reverse neighbors.
     *
     * Produces n*k pairs that are later grouped into n pairs.
     *
     * @param instancesWithNeighbors RDD[Instance]. Collection of instances to process
     * @return RDD containing a tuple for
     *         each instance with its array of reverse neighbors
     */
    def findReverseNeighbors(instancesWithNeighbors: RDD[(Int, Array[KNeighbor])]): RDD[(Int, Array[RNeighbor])] = {

        val neighborReferences = instancesWithNeighbors.flatMap(tuple => {
            val (instanceId, neighbors) = tuple
            neighbors.zipWithIndex.map(tuple => (tuple._1.id, (instanceId, tuple._2)))
        })

        val y = neighborReferences.groupByKey()
            .mapValues(rNeighbors => rNeighbors.map(
                rNeighbor => new RNeighbor(rNeighbor._1, rNeighbor._2)
            ).toArray)

        // Dealing with instances that don't have reverse neighbors and don't come
        // up in y
        instancesWithNeighbors.leftOuterJoin(y).map(tuple => {
            val (instanceId, tuple2) = tuple
            val (_, rNeighbors) = tuple2

            (instanceId, rNeighbors.getOrElse(Array[RNeighbor]()))
        })
    }
}
