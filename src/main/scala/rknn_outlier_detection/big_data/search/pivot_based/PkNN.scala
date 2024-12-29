package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.exceptions.{IncorrectKValueException, IncorrectPivotsAmountException, InsufficientInstancesException}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, OutlierPivot}
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.shared.utils.Utils.{addCandidateSupportPivot, addNewNeighbor, getKeyFromInstancesIds}
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
* This class implements a knn pivot-based search method for distributed environments based
* on the following paper. https://link.springer.com/chapter/10.1007/978-3-319-71246-8_51
*/

class PkNN(
    _pivots: Array[Instance],
    m: Int
) extends Serializable{

    def findPivotsCoefficientOfVariation(cellsLengths: Array[Int]): Double = {

        val mean = cellsLengths.sum.toDouble / cellsLengths.length.toDouble
        val std = math.sqrt(cellsLengths.map(length => math.pow(length - mean, 2)).sum / cellsLengths.length)
        std / mean

    }

    // Multistep pknn
    // 1. knn for each point of each cell -> core knn
    // 2. find core distance for each cell (max distance of a point to its k-neighbor)
    // 3. find support distance for each cell (max sum of pivot-to-instance distance + instance-to-kneighbor distance)
    // 4. find supporting cells for each cell (all cells for which support distance > half of distance between cells pivots)
    // 5. prune the support sets for each cell, eliminating all support candidates whose
    // rest between the distance (|vi, q| - |vj, q|)/2 >= core-distance(vi)
    // 6. remove outliers to balance nodes and support sets

    def getDistancesMap(pivots: Array[Instance], distanceFunction: DistanceFunction): mutable.Map[String, Double] ={
        val pivotDistances = mutable.Map[String, Double]()
        for (i <- pivots.indices){
            val i1 = pivots(i)
            for(j <- i + 1 until pivots.length){
                val i2 = pivots(j)
                val distance = distanceFunction(i1.data, i2.data)
                pivotDistances(getKeyFromInstancesIds(i1.id, i2.id)) = distance
            }
        }
        pivotDistances
    }

    /**
     * This method as described in the paper returns a threshold distance for
     * a cell, that limits the distance for instances to belong to this cell,
     * guaranteeing that no more than m instances will be proposed at a time in
     * a node
     *
     * @param radius the mean of distances from every point to the pivot
     * @param c the amount of core points in the cell
     * @param n the dimensionality of the point
     * @return the result of that equation
     */
    def findCellDistance(radius: Double, c: Int, n: Int): Double = {

        Math.pow((this.m.toDouble * Math.pow(radius, n.toDouble)) / c.toDouble, 1 / n.toDouble) - radius
    }

    def getLastNeighborDistance(kNeighbors: Array[KNeighbor]): Double = {
        kNeighbors.filter(kNeighbor => kNeighbor != null).map(_.distance).max
    }

    def findKNeighbors(instances: RDD[Instance], k: Int, distanceFunction: DistanceFunction, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {

        val pivots = sc.parallelize(_pivots)
        val instancesAmount = instances.count()
        if(instancesAmount < 2) throw new InsufficientInstancesException(s"Received less than 2 instances ($instancesAmount), not enough for a neighbors search.")
        if(_pivots.length < 1 || _pivots.length > instancesAmount) throw new IncorrectPivotsAmountException(s"Pivots amount (${_pivots.length}) has to be a number greater than 0 and up to the instances amount ($instancesAmount)")
        if(k <= 1 || k > instancesAmount - 1) throw new IncorrectKValueException(s"k ($k) has to be a natural number between 1 and n - 1 (n is instances length)")

        // Finding pivots distances and broadcasting them
        val pivotDistances = getDistancesMap(_pivots, distanceFunction)
        val broadcastedPivotDistances = sc.broadcast(pivotDistances)
        val dimensionality = instances.take(1)(0).data.length

        // Create cells
        val cells = instances.cartesian(pivots)
            .map(tuple => {
                val (instance, pivot) = tuple
                (instance, (pivot, distanceFunction(instance.data, pivot.data)))
            })
            .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
            .map(t => (t._2._1, (t._1, t._2._2)))

        cells.cache()

        // Find for each cell the (accumulated distance of points, points amount)
        val cellsAndMeanDists = cells.mapValues(t => (t._2, 1)).reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2)
        })

        val cellsAndThersholdDists = cellsAndMeanDists.map(cell => {
            val (pivot, info) = cell
            val (accumulatedDistances, amountOfPoints) = info
            val meanDistance = accumulatedDistances / amountOfPoints
            val thresholdDist = findCellDistance(meanDistance, amountOfPoints, dimensionality)
            (pivot, thresholdDist)
        })

        val thresholdedCells = cells.join(cellsAndThersholdDists)

        val unfilteredCoreKNNs = thresholdedCells.groupByKey.map(pivotAndIter => {
            val (pivot, iterable) = pivotAndIter
            val knns = new ExhaustiveSmallData().findKNeighbors(iterable.map(_._1._1).toArray, k, distanceFunction)

            val points = iterable.zip(knns).map(tuple => {
                val (secondTuple, kNeighbors) = tuple
                val (thirdTuple, thresholdDist) = secondTuple
                val (instance, distanceToPivot) = thirdTuple
                (instance, kNeighbors, distanceToPivot, getLastNeighborDistance(kNeighbors) > thresholdDist && instance.id != pivot.id)
            })

            (pivot, points)
        })

        // Remove outliers from cells
        val filteredCoreKNNs = unfilteredCoreKNNs.mapValues(cellInstances => {
            cellInstances.filter(instance => {
                val (_, _, _, outlier) = instance
                !outlier
            })
        }).filter(cell => cell._2.nonEmpty)

        // Get outliers as own cells
        val outliers = unfilteredCoreKNNs.flatMap(cell => {
            val (originPivot, instances) = cell
            instances.filter(instanceTuple => {
                val (_, _, _, isOutlier) = instanceTuple
                isOutlier
            }).map(instanceTuple => {
                val (instance, kNeighbors, _, _) = instanceTuple
                val outlierPivot: Instance = OutlierPivot.fromInstance(instance, originPivot.id)
                (outlierPivot, (instance, kNeighbors, 0.0, true))
            })
        }).groupByKey

        val coreKNNs = filteredCoreKNNs.union(outliers)
        val updatedCells = coreKNNs.flatMap(cell => {
            val (pivot, instances) = cell
            instances.map(instance => (pivot, (instance._1, instance._3)))
        })

        // Step 2
        val coreDistances = coreKNNs.mapValues(knns => {
            knns.map(tuple => {
                // knn might not be full if there are less instances for a given cell than k
                if(tuple._2.last != null) tuple._2.last.distance else Double.PositiveInfinity
            }).max
        })

        // Step 3
        val supportingDistances = coreKNNs.mapValues(iterable => {
            val kDistances = iterable.map(tuple => {
                // knn might be empty if there are less instances for a given cell than k // TODO should it be this way???
                if(tuple._2.last != null) tuple._2.last.distance + tuple._3 else Double.PositiveInfinity
            })
            if(kDistances.nonEmpty) kDistances.max else Double.PositiveInfinity
        })

        // Step 4 Find support sets

        // Get a tuple for pivot, coreDistance, supportDistance
        val pivotsAndDistances = coreDistances.join(supportingDistances).map(tuple => (tuple._1, tuple._2._1, tuple._2._2))

        def filterPivotsCombinations(tuple: ((Instance, Double, Double), (Instance, Double, Double))): Boolean = {
            val (pivot1, pivot2) = tuple
            if(pivot1._1.id == pivot2._1.id) {
                false
            }
            else{
                pivot1._1 match {
                    case op1: OutlierPivot =>
                        pivot2._1 match {
                            case op2: OutlierPivot => if(op1.originPivotId == op2.originPivotId) return false
                            case _ => if(op1.originPivotId == pivot2._1.id) return false
                        }
                    case _ => {
                        pivot2._1 match {
                            case op2: OutlierPivot => if(pivot1._1.id == op2.originPivotId) return false
                            case _ => ()
                        }
                    }
                }

                val mapKey = getKeyFromInstancesIds(pivot1._1.id, pivot2._1.id)
                val distance = if(broadcastedPivotDistances.value.contains(mapKey)) broadcastedPivotDistances.value(mapKey) else distanceFunction(pivot1._1.data, pivot2._1.data)
                distance / 2 <= pivot1._3
            }
        }
        // Get combination of pivots getting a pivot and an iterable of the pivots that it supports
        val pivotsCombinations = pivotsAndDistances.cartesian(pivotsAndDistances)
            .filter(filterPivotsCombinations)
            .map(tuple => (tuple._2._1, tuple._1))
            .groupByKey

//        val pivotCombsFor3 = pivotsCombinations.filter(cell => cell._1.id == "77").collect()(0)
//        val summary = s"For 3 pivot combinations are:\n${pivotCombsFor3._2.toArray.map(comb => s"${comb._1.id}").mkString("\n", "\n", "\n")}"
//        throw new Exception(s"For ${pivotCombsFor3._1.id} pivot combinations are:\n${pivotCombsFor3._2.toArray.map(comb => s"${comb._1.id}").mkString("\n", "\n", "\n")}")

        // Step 5 Prune candidate instances
//        var summary = ""

        val finalSupportSets = updatedCells.join(pivotsCombinations).flatMap(tuple => {
            val (candidateInstance, pivotsToSupport) = tuple._2
            pivotsToSupport.filter(pivotData => {
                val pruneMeasurement = math.abs(distanceFunction(pivotData._1.data, candidateInstance._1.data) - candidateInstance._2) / 2
//                if(candidateInstance._1.id == "21" && pivotData._1.id == "3")
//                    throw new Exception(s"Did candidate instance ${candidateInstance._1.id} make it: ${pruneMeasurement} < ${pivotData._2}\n")
                pruneMeasurement < pivotData._2
            }).map(pivotData => (pivotData._1, candidateInstance._1))
        })

//        val forZero = finalSupportSets.filter(set => set._1.id == "3")
//        throw new Exception(s"There are ${forZero.collect().length} support cells with distances: ${forZero.collect().map(tuple => s"with ${tuple._2.id} distance is ${distanceFunction(tuple._1.data, tuple._2.data)}").mkString("\n", "\n", "\n")}")

        // Correct knn search

        // pivot - which don't need after join, (ownInstance, supportInstance)
        val x = updatedCells.join(finalSupportSets)
        val mappedX = x.map(tuple => {
            val (ownInstanceData, supportInstance) = tuple._2
            val (ownInstance, _) = ownInstanceData

            (ownInstance.id, new KNeighbor(supportInstance.id, distanceFunction(ownInstance.data, supportInstance.data)))
        })
        val supportKNNs = mappedX.aggregateByKey(Array.fill[KNeighbor](k)(null))(
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

        val trimmedCoreKNNs = coreKNNs.flatMap(tuple => {
            val (_, instances) = tuple
            instances.map(instance => (instance._1.id, instance._2))
        })

        val finalKNNs = trimmedCoreKNNs.join(supportKNNs).map(tuple => {
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

        finalKNNs
    }

    def findApproximateKNeighbors(instances: RDD[Instance], k: Int, distanceFunction: (Array[Double], Array[Double]) => Double, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {
        val pivots = sc.broadcast(_pivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

//        val repartitioned = cells.repartition(cells.getNumPartitions).cache()
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
//        repartitioned.unpersist()
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

//        resulting.unpersist()
//        incompleteCoreKNNs.unpersist()
        completeCoreKNNs.union(incompleteCoreKNNsFixed)
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

    def findApproximateKNeighborsWithBroadcastedPivots(instances: RDD[Instance], k: Int, distanceFunction: DistanceFunction, sc: SparkContext): RDD[(Int, Array[KNeighbor])] = {
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
            val sortedPivots = pivotsWithCounts.value
                .map(pivot => (pivot, distanceFunction(pivot._1.data, instance.data)))
                .sortBy(_._2)

            var accumulatedCandidates = 0
            var accumulatedPivotsAmount = 0
            sortedPivots.foreach(pair => {
                if(accumulatedCandidates < k + 1){
                    accumulatedCandidates += pair._1._2
                    accumulatedPivotsAmount += 1
                }
            })

            sortedPivots.slice(0, accumulatedPivotsAmount).map(pivot => (pivot._1._1, instance))
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

}
