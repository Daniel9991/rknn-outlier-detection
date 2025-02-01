package rknn_outlier_detection.big_data.alternative_methods

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.partitioners.PivotsPartitioner
import rknn_outlier_detection.{DistanceFunction, Pivot, PivotWithCount, PivotWithCountAndDist}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.small_data.detection.{Antihub, DetectionCriteria}
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SameThingByPartition extends Serializable{

    def selectMinimumClosestPivotsRec(instance: Instance, k: Int, pivots: Array[PivotWithCountAndDist]): Array[(Instance, Instance)] = {
        @tailrec
        def minimumClosestPivotsTailRec(instance: Instance, k: Int, remainingPivots: Array[PivotWithCountAndDist], selectedPivots: ArrayBuffer[PivotWithCount]): Array[(Instance, Instance)] = {
            if(remainingPivots.isEmpty || (selectedPivots.nonEmpty && selectedPivots.map{case (_, count) => count}.sum > k)){
                selectedPivots.toArray.map{case (pivot, _) => (pivot, instance)}
            }
            else{
                val closestPivot = remainingPivots.minBy{case (_, _, distanceToPivot) => distanceToPivot}
                val formatted: PivotWithCount = (closestPivot._1, closestPivot._2)
                selectedPivots += formatted

                val updatedRemaining = remainingPivots.filter(p => p._1.id != closestPivot._1.id)
                minimumClosestPivotsTailRec(instance, k, updatedRemaining, selectedPivots)
            }
        }

        minimumClosestPivotsTailRec(instance, k, pivots, ArrayBuffer.empty[PivotWithCount])
    }

    def detectAnomalies(
       instances: RDD[Instance],
       pivotsAmount: Int,
       seed: Int,
       k: Int,
       distanceFunction: DistanceFunction,
       sc: SparkContext,
       detectionStrategy: DetectionCriteria
    ): RDD[(Int, Double)] ={
        val sample = instances.takeSample(withReplacement = false, pivotsAmount, seed)
        val pivots = sc.broadcast(sample)

        if(sample.length != pivotsAmount)
            throw new Exception("There was a different amount of sampled pivots than required")

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

//        val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)

//        val pivotsToInstance = instances.flatMap(instance => {
//            val pivotsWithCountAndDist = pivotsWithCounts.value
//                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))
//
//            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
//        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

        val result = cells.mapPartitions(iter => {
            // All elements from the same partition should belong to the same pivot
            val elements = iter.toArray.map{case (pivot, instance) => instance}
            val allKNeighbors = elements.map(el => (el.id, new ExhaustiveSmallData().findQueryKNeighbors(el, elements,k, distanceFunction)))
            val filteredKNeighbors = allKNeighbors.map{case (id, neighbors) => (id, neighbors.filter(n => n != null))}
            val rNeighbors = filteredKNeighbors.flatMap{case (instanceId, kNeighbors) =>
                kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
            }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray

            val elementsSet = mutable.HashSet.from(elements.map(_.id))
            rNeighbors.foreach(pair => elementsSet.remove(pair._1))
            val elementsWithoutRNeighbors = elementsSet.map(id => (id, Array.empty[RNeighbor])).toArray

            val outlierDegrees = detectionStrategy.scoreInstances(rNeighbors.concat(elementsWithoutRNeighbors))

            Iterator.from(outlierDegrees)
        })

        result
    }


}
