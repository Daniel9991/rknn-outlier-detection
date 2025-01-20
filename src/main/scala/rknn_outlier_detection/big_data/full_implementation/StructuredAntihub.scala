package rknn_outlier_detection.big_data.full_implementation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.{DistanceFunction, Pivot, PivotWithCount, PivotWithCountAndDist}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object StructuredAntihub {

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

    def detectFlag(
        instances: Dataset[Instance],
        pivotsAmount: Int,
        seed: Int,
        k: Int,
        distanceFunction: DistanceFunction,
        spark: SparkSession
    ): RDD[(Int, Double)] = {

        import spark.implicits._

        val sampledPivots = instances.rdd.takeSample(withReplacement = false, pivotsAmount, seed = seed)
        val pivots = spark.sparkContext.broadcast(sampledPivots)

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).cache()

        val pivotsWithCounts = spark.sparkContext.broadcast(cells.map{case (pivot, instance) => (pivot, 1)}.rdd.reduceByKey{_+_}.collect)

        val pivotsToInstance = instances.flatMap(instance => {
            val pivotsWithCountAndDist = pivotsWithCounts.value
                .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))

            selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
        })

        val pivotsToInstanceRenamed = pivotsToInstance.withColumnsRenamed(Map("_1" -> "_3", "_2" -> "_4"))

        val kNeighbors = spark.createDataset(pivotsToInstanceRenamed.join(cells, pivotsToInstanceRenamed.col("_3") === cells.col("_1"))
            .filter(col("_4") =!= col("_2")).as[(Pivot, Instance, Pivot, Instance)]
            .map(tuple => (tuple._2, KNeighbor(tuple._4.id, distanceFunction(tuple._2.data, tuple._4.data))))
            .rdd.aggregateByKey(Array.fill[KNeighbor](k)(null))(
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
            ))
            .map{case (instance, kNeighbors) => (instance.id, kNeighbors)}
            .cache()

        // Missing 0 for instances with no reverse neighbors
        val reverseCountByInstanceId = spark.createDataset(kNeighbors.flatMap{case (_, neighbors) =>
            neighbors.map(neighbor => (neighbor.id, 1))
        }.rdd.reduceByKey(_+_))

        val reverseCountByInstanceIdRenamed = reverseCountByInstanceId.withColumnsRenamed(Map("_1" -> "_3", "_2" -> "_4"))

//        // Dealing with instances that don't have reverse neighbors and don't come
//        // up in y
//        val rNeighborsCount = kNeighbors.map{case (instanceId, _) => (instanceId, 0.toByte)}
//            .join(reverseCountByInstanceId, )
//            .map{case (instanceId, (_, rNeighborsAmount))  => (instanceId, rNeighborsAmount.getOrElse(0))}
//
//        val antihub = rNeighborsCount.map{case (id, count) => {
//            (id, if(count == 0) 1.0 else 1.0 / count.toDouble)
//        }}
//
//        antihub

        spark.sparkContext.emptyRDD
    }
}
