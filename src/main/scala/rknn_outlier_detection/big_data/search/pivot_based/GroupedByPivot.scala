package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.shared.utils.Utils.addNewNeighbor

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class GroupedByPivot(_pivots: Array[Instance]) extends Serializable{

    def findApproximateKNeighbors(instances: RDD[Instance], k: Int, distanceFunction: (Array[Double], Array[Double]) => Double, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {
        val pivots = sc.broadcast(_pivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val coreKNNs = cells.join(cells)
            .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
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

        val resulting = coreKNNs

        cells.unpersist()
        resulting.cache()

        val incompleteCoreKNNs = resulting.filter(instance => {
            val (_, kNeighbors) = instance
            kNeighbors.contains(null)
        }).cache()

        if(incompleteCoreKNNs.count() == 0)
            return resulting.map(tuple => (tuple._1.id, tuple._2))

        val completeCoreKNNs = resulting.filter(instance => {
            val (_, kNeighbors) = instance
            !kNeighbors.contains(null)
        }).map(point => (point._1.id, point._2))

        val incompleteCells = incompleteCoreKNNs.map(point => {
            (point._1, point._2.filter(kNeighbor => kNeighbor != null).map(kNeighbor => kNeighbor.id))
        })

        val supportKNNs = incompleteCells.cartesian(instances)
            .filter(pair => pair._1._1.id != pair._2.id && !pair._1._2.contains(pair._2.id))
            .map(pair => (pair._1._1.id, new KNeighbor(pair._2.id, distanceFunction(pair._1._1.data, pair._2.data))))
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

        val incompleteCoreKNNsFixed = incompleteCoreKNNs.map(tuple => (tuple._1.id, tuple._2)).join(supportKNNs).map(tuple => {
            val (instanceId, knns) = tuple
            val (core, support) = knns
            val filteredSupport = if(support.contains(null)) support.filter(n => n != null) else support
            filteredSupport.foreach(supportNeighbor => {
                if(core.contains(null) || core.last.distance > supportNeighbor.distance){
                    addNewNeighbor(core, supportNeighbor)
                }
            })
            (instanceId, core)
        })

        completeCoreKNNs.union(incompleteCoreKNNsFixed)
    }

    def findApproximateKNeighborsShorty(instances: RDD[Instance], k: Int, distanceFunction: (Array[Double], Array[Double]) => Double, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {
        val pivots = sc.broadcast(_pivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val coreKNNs = cells.join(cells)
            .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
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

        coreKNNs.map{case (instance, neighbors) =>
            (instance.id, neighbors.filter(neighbor => neighbor != null))
        }
    }

    def findApproximateKNeighborsRepartitioned(instances: RDD[Instance], k: Int, distanceFunction: (Array[Double], Array[Double]) => Double, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {
        val pivots = sc.broadcast(_pivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val repartitioned = cells.repartition(cells.getNumPartitions).cache()
        val coreKNNs = repartitioned.join(repartitioned)
            .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
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

        val resulting = coreKNNs

        cells.unpersist()
        repartitioned.unpersist()
        resulting.cache()

        val incompleteCoreKNNs = resulting.filter(instance => {
            val (_, kNeighbors) = instance
            kNeighbors.contains(null)
        }).cache()

        if(incompleteCoreKNNs.count() == 0)
            return resulting.map(tuple => (tuple._1.id, tuple._2))

        val completeCoreKNNs = resulting.filter(instance => {
            val (_, kNeighbors) = instance
            !kNeighbors.contains(null)
        }).map(point => (point._1.id, point._2))

        val incompleteCells = incompleteCoreKNNs.map(point => {
            (point._1, point._2.filter(kNeighbor => kNeighbor != null).map(kNeighbor => kNeighbor.id))
        })

        val supportKNNs = incompleteCells.cartesian(instances)
            .filter(pair => pair._1._1.id != pair._2.id && !pair._1._2.contains(pair._2.id))
            .map(pair => (pair._1._1.id, new KNeighbor(pair._2.id, distanceFunction(pair._1._1.data, pair._2.data))))
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

        val incompleteCoreKNNsFixed = incompleteCoreKNNs.map(tuple => (tuple._1.id, tuple._2)).join(supportKNNs).map(tuple => {
            val (instanceId, knns) = tuple
            val (core, support) = knns
            val filteredSupport = if(support.contains(null)) support.filter(n => n != null) else support
            filteredSupport.foreach(supportNeighbor => {
                if(core.contains(null) || core.last.distance > supportNeighbor.distance){
                    addNewNeighbor(core, supportNeighbor)
                }
            })
            (instanceId, core)
        })

        resulting.unpersist()
        incompleteCoreKNNs.unpersist()
        completeCoreKNNs.union(incompleteCoreKNNsFixed)
    }

    type PivotWithCountAndDist = (Instance, Int, Double)
    type PivotWithCount = (Instance, Int)

    def findApproximateKNeighborsWithBroadcastedPivots(instances: RDD[Instance], k: Int, distanceFunction: DistanceFunction, sc: SparkContext, tailrec: Boolean = true): RDD[(Int, Array[KNeighbor])] = {

        val pivots = sc.broadcast(_pivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        })

        val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)

        val instanceToPivots = instances.flatMap(instance => {
            val pivotsWithCountAndDist = pivotsWithCounts.value
                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))

            if(tailrec)
                selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
            else
                selectMinimumClosestPivotsIter(instance, k, pivotsWithCountAndDist)
        })

        val coreKNNs = instanceToPivots.join(cells)
            .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
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

        coreKNNs.map{case (instance, kNeighbors) => (instance.id, kNeighbors)}
    }

    def selectMinimumClosestPivotsIter(instance: Instance, k: Int, pivots: Array[PivotWithCountAndDist]): Array[(Instance, Instance)] = {
        val sortedPivots = pivots
            .sortBy{case (_, _, dist) => dist}

        var accumulatedCount = 0
        var accumulatedPivots = 0
        sortedPivots.foreach(pivot => {
            if(accumulatedCount < k + 1){
                accumulatedCount += pivot._2
                accumulatedPivots += 1
            }
        })

        sortedPivots.slice(0, accumulatedPivots).map(pivot => (pivot._1, instance))
    }

    def selectMinimumClosestPivotsRec(instance: Instance, k: Int, pivots: Array[PivotWithCountAndDist]): Array[(Instance, Instance)] = {
        @tailrec
        def minimumClosestPivotsTailRec(instance: Instance, k: Int, remainingPivots: Array[PivotWithCountAndDist], selectedPivots: ArrayBuffer[PivotWithCount]): Array[(Instance, Instance)] = {
            if(remainingPivots.isEmpty || (selectedPivots.nonEmpty && selectedPivots.map{case (_, count) => count}.sum >= k)){
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
}
