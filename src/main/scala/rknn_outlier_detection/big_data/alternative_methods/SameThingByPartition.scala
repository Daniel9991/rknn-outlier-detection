package rknn_outlier_detection.big_data.alternative_methods

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.{DistanceFunction, Pivot}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.small_data.detection.Antihub
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

object SameThingByPartition extends Serializable{
    def detectAnomalies(
       instances: RDD[Instance],
       pivotsAmount: Int,
       seed: Int,
       k: Int,
       distanceFunction: DistanceFunction,
       sc: SparkContext
    ): RDD[(Int, Double)] ={
        val sample = instances.takeSample(false, pivotsAmount, seed)
        val pivots = sc.broadcast(sample)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        })

        val result = cells.mapPartitions(iter => {
            val allKNeighbors = (iter.toArray.groupMap{case (pivot, _) => pivot.id}{case (_, instance) => instance})
                .values.flatMap(compactGroup => {
                    compactGroup.map(instance => (instance.id, new ExhaustiveSmallData().findQueryKNeighbors(instance, compactGroup, k, distanceFunction).filter(n => n != null)))
            })

            val rNeighbors = allKNeighbors.flatMap{case (instanceId, kNeighbors) =>
                kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
            }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}

            val outlierDegs = rNeighbors.map{case (instanceId, rNeighbors) => (instanceId, Antihub.normalizeReverseNeighborsCount(rNeighbors.toArray.length))}

            Iterator.from(outlierDegs)
        })

        result
    }
}
