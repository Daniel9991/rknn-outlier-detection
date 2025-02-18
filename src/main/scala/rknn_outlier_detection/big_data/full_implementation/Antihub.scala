package rknn_outlier_detection.big_data.full_implementation

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.partitioners.PivotsPartitioner
import rknn_outlier_detection.{DistanceFunction, Pivot, PivotWithCount, PivotWithCountAndDist}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.annotation.tailrec
import scala.collection.mutable
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

    // Regular antihub
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
            .map(tuple => (tuple._2._1.id, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
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

        cells.unpersist()
        kNeighbors.unpersist()
        antihub
    }

    // Antihub where join for reverse neighbors completion is using (id, byte)
    def detectLightweightJoin(
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
            .map(tuple => (tuple._2._1.id, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
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

        cells.unpersist()
        kNeighbors.unpersist()
        antihub
    }

    // Method for completion creates a HashSet with all ids and removes all ids with at least one RNeighbor
    def detectBasicHashSet(
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
            .map(tuple => (tuple._2._1.id, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
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
            .cache()

        // Missing 0 for instances with no reverse neighbors
        val reverseCountByInstanceId = kNeighbors.flatMap{case (_, neighbors) =>
            neighbors.map(neighbor => (neighbor.id, 1))
        }.reduceByKey(_+_)

        // Dealing with instances that don't have reverse neighbors and don't come
        // up in y
//        val rNeighborsCount = kNeighbors.map{case (instanceId, _) => (instanceId, 0.toByte)}
//            .leftOuterJoin(reverseCountByInstanceId)
//            .map{case (instanceId, (_, rNeighborsAmount))  => (instanceId, rNeighborsAmount.getOrElse(0))}

        val antihub = reverseCountByInstanceId.map{case (id, count) => {
            (id, if(count == 0) 1.0 else 1.0 / count.toDouble)
        }}

        cells.unpersist()
        kNeighbors.unpersist()

        val allIds = instances.map(_.id).collect().to(scala.collection.mutable.HashSet)
        reverseCountByInstanceId.map(_._1).collect().foreach(id => allIds.remove(id))
        val fullAntihub = antihub.union(sc.parallelize(allIds.toSeq.map(id => (id, 0))))

        fullAntihub
    }

    // Antihub where join for reverse neighbors completion is using (id, byte)
    // + using a repartition and methods to respect that partition (in order to avoid costly joins)
    def detectRepartitionedLightweightJoin(
        instances: RDD[Instance],
        pivotsAmount: Int,
        seed: Int,
        k: Int,
        distanceFunction: DistanceFunction,
        sc: SparkContext
    ): RDD[(Int, Double)] = {


        val sampledPivots = instances.takeSample(withReplacement = false, pivotsAmount, seed = seed)
        val pivots = sc.broadcast(sampledPivots)

        val customPartitioner = new PivotsPartitioner(pivotsAmount, sampledPivots.map(_.id))

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
        }).partitionBy(customPartitioner)

        val byPartitionKNeighbors = pivotsToInstance.mapPartitions(iter => {
            // All elements from the same partition should belong to the same pivot
            val elements = iter.toArray.map{case (pivot, instance) => instance}

            // There can be elements with null kNeighbors
            val allKNeighbors = elements.map(el => (el.id, new ExhaustiveSmallData().findQueryKNeighbors(el, elements,k, distanceFunction)))

            Iterator.from(allKNeighbors)
        })

        val finalKNeighbors = byPartitionKNeighbors
            .reduceByKey(
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
            .persist()

        // Missing 0 for instances with no reverse neighbors
        val reverseCountByInstanceId = finalKNeighbors.flatMap{case (_, neighbors) =>
            neighbors.map(neighbor => (neighbor.id, 1))
        }.reduceByKey(_+_)

        // Dealing with instances that don't have reverse neighbors and don't come
        // up in y
        val rNeighborsCount = finalKNeighbors.mapValues(_ => 0.toByte)
            .leftOuterJoin(reverseCountByInstanceId)
            .mapValues{case (_, rNeighborsAmount)  => rNeighborsAmount.getOrElse(0)}

        val antihub = rNeighborsCount.mapValues(count =>
            if(count == 0) 1.0 else 1.0 / count.toDouble
        )

        finalKNeighbors.unpersist()
        antihub
    }
}
