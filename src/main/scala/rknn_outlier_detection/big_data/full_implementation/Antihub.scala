package rknn_outlier_detection.big_data.full_implementation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.{DistanceFunction, Pivot, PivotWithCount, PivotWithCountAndDist}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.Utils

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Antihub {

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

    def detect(
      instances: RDD[Instance],
      pivotsAmount: Int,
      seed: Int,
      k: Int,
      distanceFunction: DistanceFunction,
      sc: SparkContext
    ): RDD[(Int, Double)] = {


        val sampledPivots = instances.takeSample(withReplacement = false, pivotsAmount, seed = seed)
        val pivots = sc.broadcast(sampledPivots)

        // Create cells
        val cells: RDD[(Pivot, Instance)] = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)

        val pivotsToInstance = instances.flatMap(instance => {
            val pivotsWithCountAndDist = pivotsWithCounts.value
                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))

            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
        })

        val kNeighbors = pivotsToInstance.join(cells)
            .filter{case (_, (ins1, ins2)) => ins1.id != ins2.id}
            .map(tuple => (tuple._2._1, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
            .aggregateByKey(Array.fill[KNeighbor](k)(null))(
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
            .map{case (instance, kNeighbors) => (instance.id, kNeighbors)}
            .cache()

        // Missing 0 for instances with no reverse neighbors
        val reverseCountByInstanceId = kNeighbors.flatMap{case (_, neighbors) =>
            neighbors.map(neighbor => (neighbor.id, 1))
        }.reduceByKey(_+_)

        // Dealing with instances that don't have reverse neighbors and don't come
        // up in y
        val rNeighbors = kNeighbors.leftOuterJoin(reverseCountByInstanceId)
            .map(tuple => {
                val (instanceId, tuple2) = tuple
                val (_, rNeighbors) = tuple2

                (instanceId, rNeighbors.getOrElse(0))
        })

        val antihub = rNeighbors.map{case (id, count) => {
            (id, if(count == 0) 1.0 else 1.0 / count.toDouble)
        }}

        antihub
    }

    def detectFlag(
        instances: RDD[Instance],
        pivotsAmount: Int,
        seed: Int,
        k: Int,
        distanceFunction: DistanceFunction,
        sc: SparkContext
    ): RDD[(Int, Double)] = {


        val sampledPivots = instances.takeSample(withReplacement = false, pivotsAmount, seed = seed)
        val pivots = sc.broadcast(sampledPivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)

        val pivotsToInstance = instances.flatMap(instance => {
            val pivotsWithCountAndDist = pivotsWithCounts.value
                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))

            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
        })

        val kNeighbors = pivotsToInstance.join(cells)
            .filter{case (_, (ins1, ins2)) => ins1.id != ins2.id}
            .map(tuple => (tuple._2._1, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
            .aggregateByKey(Array.fill[KNeighbor](k)(null))(
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
            .map{case (instance, kNeighbors) => (instance.id, kNeighbors)}
            .cache()

        // Missing 0 for instances with no reverse neighbors
        val reverseCountByInstanceId = kNeighbors.flatMap{case (_, neighbors) =>
            neighbors.map(neighbor => (neighbor.id, 1))
        }.reduceByKey(_+_)

        // Dealing with instances that don't have reverse neighbors and don't come
        // up in y
        val rNeighborsCount = kNeighbors.map{case (instanceId, _) => (instanceId, 0.toByte)}
            .leftOuterJoin(reverseCountByInstanceId)
            .map{case (instanceId, (_, rNeighborsAmount))  => (instanceId, rNeighborsAmount.getOrElse(0))}

        val antihub = rNeighborsCount.map{case (id, count) => {
            (id, if(count == 0) 1.0 else 1.0 / count.toDouble)
        }}

        antihub
    }
}
