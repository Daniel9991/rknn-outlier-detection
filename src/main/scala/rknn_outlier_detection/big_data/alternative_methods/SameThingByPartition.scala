package rknn_outlier_detection.big_data.alternative_methods

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import rknn_outlier_detection.big_data.partitioners.PivotsPartitioner
import rknn_outlier_detection.big_data.search.pivot_based.{FarthestFirstTraversal, IncrementalSelection}
import rknn_outlier_detection.{DistanceFunction, Pivot, PivotWithCount, PivotWithCountAndDist, selectMinimumClosestPivotsRec}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.detection.{Antihub, DetectionStrategy}
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SameThingByPartition extends Serializable{

    def detectAnomalies(
       instances: RDD[Instance],
       pivotsAmount: Int,
       seed: Int,
       k: Int,
       distanceFunction: DistanceFunction,
       sc: SparkContext,
       detectionStrategy: DetectionStrategy,
       selectedPivots: Array[Instance],
    ): RDD[(Int, Double)] ={

        val pivots = sc.broadcast(selectedPivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

//        val pivotsToInstance = instances.flatMap(instance => {
//            val pivotsWithCountAndDist = pivotsWithCounts.value
//                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))
//
//            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
//        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

//        val countByPivot = cells.map{case (pivot, instance) => (pivot.id, 1)}.reduceByKey(_+_)

//        throw new Exception(s"\nPivots ${countByPivot.count} (from ${pivots.value.length}) with counts: ${countByPivot.sortBy(_._2, ascending = false).collect().mkString("\n", ",\n", "\n")}")

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

    // Sending points from small cells to second closest cells
    def detectAnomaliesEntrometidos(
                           instances: RDD[Instance],
                           pivotsAmount: Int,
                           seed: Int,
                           k: Int,
                           distanceFunction: DistanceFunction,
                           sc: SparkContext,
                           detectionStrategy: DetectionStrategy,
                           selectedPivots: Array[Instance],
                       ): RDD[(Int, Double)] ={

        val pivots = sc.broadcast(selectedPivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).persist()

        val countPerCell = cells.countByKey()
        val countPerCellBC = sc.broadcast(countPerCell)
        val smallPivots = countPerCell.filter{case (pivot, count) => count <= k}.toSet.map(_._1)
        val okPoints = cells.filter{case (pivot, point) => !smallPivots.contains(pivot)}
        val notOkPoints = cells.filter{case (pivot, point) => smallPivots.contains(pivot)}.map(_._2)

        val pivotsToNotOkPoints = notOkPoints.flatMap(point => {
            val pivotsDistance = pivots.value.map(pivot => (pivot, distanceFunction(pivot.data, point.data), countPerCellBC.value(pivot)))
            val minimumPivots = ArrayBuffer.empty[Instance]
            var counter = 0
            var neighbors = 0
            while(neighbors <= k && counter <= pivotsDistance.length){
                minimumPivots += pivotsDistance(counter)._1
                neighbors += pivotsDistance(counter)._3
                counter += 1
            }

            minimumPivots.toArray.map(pivot => (pivot, point))
        })

        val allPoints = okPoints.union(pivotsToNotOkPoints)
            .partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val kNeighbors = allPoints.mapPartitions(iter => {
            // All elements from the same partition should belong to the same pivot
            val elements = iter.toArray.map{case (pivot, instance) => instance}
            val allKNeighbors = elements.map(el => (el.id, new ExhaustiveSmallData().findQueryKNeighbors(el, elements,k, distanceFunction)))

            Iterator.from(allKNeighbors)
        })

        //        val pivotsToInstance = instances.flatMap(instance => {
        //            val pivotsWithCountAndDist = pivotsWithCounts.value
        //                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))
        //
        //            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
        //        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

        //        val countByPivot = cells.map{case (pivot, instance) => (pivot.id, 1)}.reduceByKey(_+_)

        //        throw new Exception(s"\nPivots ${countByPivot.count} (from ${pivots.value.length}) with counts: ${countByPivot.sortBy(_._2, ascending = false).collect().mkString("\n", ",\n", "\n")}")

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

    def notDetectAnomalies(
       instances: RDD[Instance],
       pivotsAmount: Int,
       seed: Int,
       k: Int,
       distanceFunction: DistanceFunction,
       sc: SparkContext,
       detectionStrategy: DetectionStrategy,
       selectedPivots: Array[Instance],
    ): RDD[(Int, Double)] ={

        val pivots = sc.broadcast(selectedPivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        })

//        val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)

//        val pivotsToInstance = instances.flatMap(instance => {
//            val pivotsWithCountAndDist = pivotsWithCounts.value
//                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))
//
//            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
//        }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

//        val countByPivot = cells.map{case (pivot, instance) => (pivot.id, 1)}.reduceByKey(_+_)

//        throw new Exception(s"\nPivots ${countByPivot.count} (from ${pivots.value.length}) with counts: ${countByPivot.sortBy(_._2, ascending = false).collect().mkString("\n", ",\n", "\n")}")

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

    /*
        After assigning points to each pivot
     */
    def detectAnomaliesNoOneLeftBehind(
       instances: RDD[Instance],
       pivotsAmount: Int,
       seed: Int,
       k: Int,
       distanceFunction: DistanceFunction,
       sc: SparkContext,
       detectionStrategy: DetectionStrategy,
       selectedPivots: Array[Instance]
    ): RDD[(Int, Double)] ={
        val pivots = sc.broadcast(mutable.HashSet.from(selectedPivots))

        val nonPivots = instances.filter(instance => !pivots.value.contains(instance)).persist()

        // Create cells
        val instancesWithPivotDistances = nonPivots.map(instance => {
            val sortedPivots = pivots.value.toArray
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .sortWith{case ((_, dist1), (_, dist2)) => dist2 > dist1}
                .map(_._1)

            (instance, sortedPivots)
        })

        val pivotToInstance = instancesWithPivotDistances
            .map{case (instance, sortedPivots) => (sortedPivots(0), instance)}
            .union(sc.parallelize(selectedPivots.map(pivot => (pivot, pivot))))

        val pivotsWithCount = pivotToInstance.map{case (pivot, _) => (pivot, 1)}.reduceByKey(_+_)
        val needyPivots = pivotsWithCount.filter{case (_, count) => count < k + 1}
        // Ok up to this point

        val supportingPivotsToNeedy = needyPivots.cartesian(pivotsWithCount).coalesce(sc.defaultParallelism)
            .filter{case ((needy, _), (otherPivot, _)) => needy.id != otherPivot.id}
            .groupByKey()
            .flatMap{case ((needy, corePointsCount), otherPivotsWithCounts) => {
                val pivotsWithCountAndDistance = otherPivotsWithCounts.map{
                    case (otherPivot, count) => (otherPivot, count, distanceFunction(needy.data, otherPivot.data))
                }.toArray.sortWith((t1, t2) => t2._3 > t1._3)

                val neededPivots = ArrayBuffer.empty[(Instance, Int, Double)]
                var count = corePointsCount
                var i = 0
                while(count < k + 1 && i < pivotsWithCountAndDistance.length){
                    neededPivots += pivotsWithCountAndDistance(i)
                    count += pivotsWithCountAndDistance(i)._2
                    i += 1
                }

                neededPivots.toArray.map(t => (t._1, needy))
            }}
            .aggregateByKey(ArrayBuffer.empty[Instance])((buffer, needyPivot) => buffer.clone() += needyPivot, (buffer1, buffer2) => buffer1.clone().concat(buffer2.clone()))
            .collect()
            .map{case (pivot, needyPivots) => (pivot, needyPivots.toArray)}

        val supportingPivotsToNeedyMap = Map.from(supportingPivotsToNeedy)
        val broadcastedPivotsToNeedy = sc.broadcast(supportingPivotsToNeedyMap)

        val allInstances = pivotToInstance.flatMap{case (pivot, instance) => {
            val thisPivotSupportsOthers = broadcastedPivotsToNeedy.value.contains(pivot)

            if(!thisPivotSupportsOthers) {
                Array((pivot, (instance, true)))
            } else {
                val needies: Array[Instance] = broadcastedPivotsToNeedy.value.getOrElse(pivot, Array.empty[Instance])
                Array((pivot, (instance, true))).concat(needies.map(needy => (needy, (instance, false))))
            }
        }}.partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id).toArray)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val resultingPivotsAmount = allInstances.map(t => (t._1, 1)).reduceByKey(_+_).count
        if(resultingPivotsAmount != selectedPivots.length){
            throw new Exception(s"There is a different amount of pivots after calculation: ${selectedPivots.length} vs $resultingPivotsAmount")
        }

        val result = allInstances.mapPartitions(iter => {
            // All elements from the same partition should belong to the same pivot
            val exhaustiveSmallData = new ExhaustiveSmallData
            val arr = iter.toArray
            val allElements = arr.map{case (pivot, (instance, isCore)) => instance}
            val coreElements = arr.filter{case (pivot, (instance, isCore)) => isCore}.map{case (pivot, (instance, isCore)) => instance}
            val allKNeighbors = coreElements.map(el => (el.id, exhaustiveSmallData.findQueryKNeighbors(el, allElements,k, distanceFunction)))
            val filteredKNeighbors = allKNeighbors.map{case (id, neighbors) => (id, neighbors.filter(n => n != null))}
            val rNeighbors = filteredKNeighbors.flatMap{case (instanceId, kNeighbors) =>
                kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
            }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray

            val elementsSet = mutable.HashSet.from(coreElements.map(_.id))
            rNeighbors.foreach(pair => elementsSet.remove(pair._1))
            val elementsWithoutRNeighbors = elementsSet.map(id => (id, Array.empty[RNeighbor])).toArray

            val outlierDegrees = detectionStrategy.scoreInstances(rNeighbors.concat(elementsWithoutRNeighbors))

            Iterator.from(outlierDegrees)
        })

        result
    }


}
