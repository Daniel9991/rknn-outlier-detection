package rknn_outlier_detection.big_data.search.exhaustive_knn

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.{DistanceFunction, euclidean}
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.exceptions.{IncorrectKValueException, InsufficientInstancesException}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.shared.utils.Utils.sortNeighbors

class ExhaustiveBigData[A] extends KNNSearchStrategy[A] {

    /**
     * Find k neighbors by using the cartesian product to get all
     * combinations of instances.
     * Filter out pairs where both instances are the same.
     * Map all pairs to KNeighbor objects
     * Group all neighbors by instance
     * Sort all neighbors for an instance and slice it to get
     * the k closest instances
     *
     * Costly as it produces lots of pairs (n squared).
     * Sorts n arrays of size n-1 to get k neighbors (n being instances.length)
     *
     * @param instances Collection of instances to process
     * @param k Amount of neighbors for each instance
     * @return RDD containing a tuple for
     *         each instance with its array of neighbors
     */
    def findKNeighborsMappingAllToKNeighbors(instances: RDD[Instance[A]], k: Int, distanceFunction: DistanceFunction[A]): RDD[(String, Array[KNeighbor])]={

        val fullyMappedInstances = instances.cartesian(instances)
            .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
            .map(instances_tuple => {
                val (ins1, ins2) = instances_tuple
                (
                ins1.id,
                new KNeighbor(
                ins2.id,
                    distanceFunction(ins1.attributes, ins2.attributes)
                )
                )
            })

        val groupedCombinations = fullyMappedInstances.groupByKey()
        val x = groupedCombinations.map(tuple => {
            val (instanceId, neighbors) = tuple
            (
            instanceId,
            neighbors.toArray.sortWith(sortNeighbors).slice(0, k)
            )
        })

        x
    }

    /**
     * Find k neighbors by using the cartesian product to get all
     * combinations of instances.
     * Filter out pairs where both instances are the same.
     * Map all pairs to KNeighbor objects
     * Aggregate pairs of (instanceId, KNeighbor) by maintaining
     * an array of k positions that serves to discard instances that
     * are far and include (by replacement) instances that are near.
     *
     * Costly as it produces lots of pairs (n squared).
     *
     * @param instances Collection of instances to process
     * @param k Amount of neighbors for each instance
     * @return RDD containing a tuple for
     *         each instance with its array of neighbors
     */
    def findKNeighborsAggregatingPairs(instances: RDD[Instance[A]], k: Int, distanceFunction: DistanceFunction[A]): RDD[(String, Array[KNeighbor])]={

        val fullyMappedInstances = instances.cartesian(instances)
            .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
            .map(instances_tuple => {
                val (ins1, ins2) = instances_tuple
                (
                    ins1.id,
                    new KNeighbor(
                        ins2.id,
                        distanceFunction(ins1.attributes, ins2.attributes)
                    )
                )
            })

        val x = fullyMappedInstances.aggregateByKey(Array.fill[KNeighbor](k)(null))(
            (acc, neighbor) => {
                var finalAcc = acc
                if(acc.last == null || neighbor.distance < acc.last.distance)
                    finalAcc = Utils.insertNeighborInArray(acc, neighbor)

                finalAcc
            },
            (acc1, acc2) => {
                var finalAcc = acc1
                for(neighbor <- acc2){
                    if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                        finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                    }
                }

                finalAcc
            }
        )

        x
    }

    override def findKNeighbors(instances: RDD[Instance[A]], k: Int, distanceFunction: DistanceFunction[A], sc: SparkContext): RDD[(String, Array[KNeighbor])] = {
        val instancesAmount = instances.count()
        if(instancesAmount < 2) throw new InsufficientInstancesException("Received less than 2 instances, not enough for a neighbors search.")
        if(k <= 1 || k > instancesAmount - 1) throw new IncorrectKValueException("k has to be a natural number between 1 and n - 1 (n is instances length)")
        findKNeighborsAggregatingPairs(instances, k, distanceFunction)
    }
}
