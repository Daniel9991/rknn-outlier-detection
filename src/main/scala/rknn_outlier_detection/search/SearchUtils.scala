package rknn_outlier_detection.search

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{KNeighbor, Neighbor}

object SearchUtils {
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
    def findReverseNeighbors(instancesWithNeighbors: RDD[(String, Array[KNeighbor])]): RDD[(String, Array[Neighbor])]={
        // TODO What happens with instances that don't have a reverse neighbors???

        val neighborReferences = instancesWithNeighbors.flatMap(tuple => {
            val (instanceId, neighbors) = tuple
            neighbors.map(neighbor => (neighbor.id, instanceId))
        })

        val y = neighborReferences.groupByKey()
            .mapValues(rNeighbors => rNeighbors.map(
                rNeighbor => new Neighbor(rNeighbor)
            ).toArray)

        y
    }
}
