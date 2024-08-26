package rknn_outlier_detection.small_data.search.pivot_based

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BalancingPivotsOcurrences (
    objectSet: Array[Instance],
    candidatePivots: Array[Instance],
    distanceFunction: DistanceFunction,
    removingCandidatesRatio: Double = 1.0
){

    val distances = mutable.Map[String, Double]()

    for(obj <- objectSet){
        for(pivot <- candidatePivots){
            val distance = distanceFunction(obj.data, pivot.data)
            val key = s"${pivot.id}_${obj.id}"
            distances(key) = distance
        }
    }

    def findPivots(
        pivotsAmount: Int,
    ): Array[Instance] = {

        var finalPivots = candidatePivots
        var iterations = 1

        while(finalPivots.length > pivotsAmount){
            println(s"----- Iteration start #$iterations ------")
            println(s"Candidate Pivots Amount: ${finalPivots.length}")
            println(s"Still need to trim: ${finalPivots.length - pivotsAmount}")

            val candidatePivotsToRemove = Random.shuffle(finalPivots.toSeq).toArray.take(math.round(finalPivots.length * removingCandidatesRatio).toInt)

            var worstPivot = candidatePivotsToRemove(0)
            var worstStd = std(candidatePivotsToRemove.slice(1, finalPivots.length))

            for(index <- 1 until candidatePivotsToRemove.length){
                val currentPivot = candidatePivotsToRemove(index)
                val currentStd = std(finalPivots.filter(p => p.id != currentPivot.id))
                if(currentStd < worstStd){
                    worstStd = currentStd
                    worstPivot = currentPivot
                }
            }

            finalPivots = finalPivots.filter(p => p.id != worstPivot.id)
            iterations += 1
        }

        finalPivots
    }

    def std(pivots: Array[Instance]): Double = {
        println("Calculating std")

        val permutationRanks = new ArrayBuffer[Double]()

        for(k <- 1 to pivots.length){
            pivots.foreach(pivot => {
                val rank = permutationRank(pivot, k, pivots)
                permutationRanks.addOne(rank)
            })
        }

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
        pivots: Array[Instance]
    ): Int = {
        // For each object where pivotRank of pivot and object equals k, add 1
        val values = objectSet.map(obj => {
            if(pivotRank(pivot, obj, pivots) == k) 1
            else 0
        }).sum

        values
    }
}
