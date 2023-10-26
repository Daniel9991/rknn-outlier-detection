package rknn_outlier_detection.search

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}
import rknn_outlier_detection.utils.{DistanceFunctions, Utils}
import rknn_outlier_detection.utils.Utils.sortNeighbors

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object ExhaustiveSearch {

    def findKNeighborsForAll(instances: RDD[Instance], k: Int): RDD[(String, Array[KNeighbor])]={

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

        val instancesWithKNeighbors = fullyMappedInstances.aggregateByKey(Array.fill[KNeighbor](k)(null))(
            (acc, neighbor) => {
                if(acc.last == null || neighbor.distance < acc.last.distance) Utils.insertNeighborInArray(acc, neighbor)
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

        instancesWithKNeighbors
    }

    def getKNeighbors(instances: RDD[Instance], k: Int): RDD[(String, Array[KNeighbor])]={

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
                neighbors.toArray.sortWith(sortNeighbors).slice(0, k))
        })

        x
    }

    def getReverseNeighbors(instancesWithNeighbors: RDD[(String, Array[KNeighbor])]): RDD[(String, Array[Neighbor])]={
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
