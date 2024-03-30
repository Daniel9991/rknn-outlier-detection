package rknn_outlier_detection.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}
import rknn_outlier_detection.distance.DistanceFunctions
import rknn_outlier_detection.utils.Utils
import rknn_outlier_detection.utils.Utils.sortNeighbors

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object ExhaustiveBigData extends KNNSearchStrategy {

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
    def findKNeighborsMappingAllToKNeighbors(instances: RDD[Instance], k: Int): RDD[(String, Array[KNeighbor])]={

        val fullyMappedInstances = instances.cartesian(instances)
            .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
            .map(instances_tuple => {
                val (ins1, ins2) = instances_tuple
                (
                ins1.id,
                new KNeighbor(
                ins2.id,
                DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)
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
    def findKNeighborsAggregatingPairs(instances: RDD[Instance], k: Int): RDD[(String, Array[KNeighbor])]={

        val fullyMappedInstances = instances.cartesian(instances)
            .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
            .map(instances_tuple => {
                val (ins1, ins2) = instances_tuple
                (
                    ins1.id,
                    new KNeighbor(
                        ins2.id,
                        DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)
                    )
                )
            })

        val x = fullyMappedInstances.aggregateByKey(Array.fill[KNeighbor](k)(null))(
            (acc, neighbor) => {
                if(acc.last == null || neighbor.distance < acc.last.distance)
                    Utils.insertNeighborInArray(acc, neighbor)

                acc
            },
            (acc1, acc2) => {
                var finalAcc = acc1
                for(neighbor <- acc2){
                    if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                        finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                    }
                    else{
                        break
                    }
                }

                finalAcc
            }
        )

        x
    }

    override def findKNeighbors(instances: RDD[Instance], k: Int, sc: SparkContext): RDD[(String, Array[KNeighbor])] = {
        findKNeighborsAggregatingPairs(instances, k)
    }
}
