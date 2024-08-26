package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BalancingPivotsOccurrences(
   val objectSet: RDD[Instance],
   val candidatePivots: RDD[Instance],
   val distanceFunction: DistanceFunction,
   val removingCandidatesRatio: Double = 1.0
) extends Serializable {

    val distances: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val localObjects: Array[Instance] = objectSet.collect()
    val localCandidatePivots: Array[Instance] = candidatePivots.collect()

    for(obj <- localObjects){
        for(pivot <- localCandidatePivots){
            val distance = distanceFunction(obj.data, pivot.data)
            val key = s"${pivot.id}_${obj.id}"
            distances(key) = distance
        }
    }

    def findPivots(
      pivotsAmount: Int,
      sc: SparkContext
  ): RDD[Instance] = {

        var finalPivots = candidatePivots
        var iterations = 1

        while(finalPivots.count > pivotsAmount){

            val candidatesToRemove = sc.parallelize(finalPivots.take(math.round(finalPivots.count() * removingCandidatesRatio).toInt))
            val pivotSets = candidatesToRemove.cartesian(finalPivots)
                .filter(tuple => tuple._1.id != tuple._2.id)
                .groupByKey.mapValues(_.toArray)

            val stds = pivotSets.mapValues(pivotSet => std(pivotSet, localObjects))

            val minStdPivot = stds.reduce((t1, t2) => if(t1._2 <= t2._2) t1 else t2)._1

            finalPivots = finalPivots.filter(pivot => pivot.id != minStdPivot.id)
        }

        finalPivots
    }

    def std(pivots: Array[Instance], objects: Array[Instance]): Double = {
        println("Calculating std")

        val permutationRanks = new ArrayBuffer[Double]()

        pivots.indices.foreach(index => {
            pivots.foreach(pivot => {
//                val rank = permutationRank(pivot, index + 1, pivots, objects)
                val rank = Random.nextInt(pivots.length)
                permutationRanks.addOne(rank)
            })
        })

        val mean = permutationRanks.sum / permutationRanks.length
        val sum = permutationRanks.map(rank => math.pow(rank - mean, 2)).sum

        sum / math.pow(pivots.length, 2)
    }

    def pivotRank(
                     pivot: Instance,
                     obj: Instance,
                     pivots: Array[Instance]
                 ): Int = {
        // Add 1 for each pivot that is closer to the obj than the query pivot
        val pivotDistToObj = distances(s"${pivot.id}_${obj.id}")
        val values = pivots.map(p => if(distances(s"${p.id}_${obj.id}") < pivotDistToObj) 1 else 0)

        1 + values.sum
    }

    def permutationRank(
                           pivot: Instance,
                           k: Int,
                           pivots: Array[Instance],
                           objects: Array[Instance]
                       ): Int = {
        // For each object where pivotRank of pivot and object equals k, add 1
        val values = objects.map(obj => {
            if(pivotRank(pivot, obj, pivots) == k) 1
            else 0
        }).sum

        values
    }
}

object BalancingPivotsOccurrences{

}