package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @param candidatePivots the set of instances from which to select the pivots. Its length has to be greater than m
 * @param objectPairs the set of instances tuples to test the distance distribution resulting from the selected candidate
 * @param m the amount of pivots to select
 *
 * A greater amount of tuples in objectPairs (supposedly) yields better results than a greater candidate set
 */
class IncrementalSelection (
   candidatesAmount: Int,
   objectPairsAmount: Int,
   amountOfPivotSets: Int
) extends PivotSelector{

    val random = new Random(345)

    override def findPivots(
       instances: RDD[Instance],
       pivotsAmount: Int,
       distanceFunction: DistanceFunction
   ): Array[Instance] = {

        val instancesAmount = instances.count()

        if(candidatesAmount >= instancesAmount){
            throw new Exception("The amount of pivot candidates has to be less than the amount of instances")
        }
        if(objectPairsAmount >= instancesAmount){
            throw new Exception("The amount of objectPairs to select has to be less than the amount of instances")
        }
        if(pivotsAmount >= candidatesAmount){
            throw new Exception("The amount of pivots to select has to be less than the length of candidates set")
        }

        val objectPairsLeft = instances.takeSample(withReplacement = false, objectPairsAmount, 345)
        val objectPairsRight = instances.takeSample(withReplacement = false, objectPairsAmount, 345)
        val objectPairs = objectPairsLeft.zip(objectPairsRight)

        val pivotSets = instances.mapPartitions(instances => {
            val candidatePivots = instances.take(candidatesAmount)
            val smallDataIS = new rknn_outlier_detection.small_data.search.pivot_based.IncrementalSelection(candidatePivots.toArray, objectPairs)
            val pivots = smallDataIS.findPivots(Array(),distanceFunction,pivotsAmount)
            Array(pivots).iterator
        })

        val finalPivotSet = pivotSets.reduce(
            (set1, set2) => if(rknn_outlier_detection.small_data.search.pivot_based.IncrementalSelection.findBestPivotSet(set1, set2, objectPairs, distanceFunction)) set1 else set2
        )

        finalPivotSet
    }
}

