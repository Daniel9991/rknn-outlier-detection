package rknn_outlier_detection.small_data.search.pivot_based

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
   candidatePivots: Array[Instance],
   objectPairs: Array[(Instance, Instance)],
) extends PivotSelector{

    val random = new Random()

    override def findPivots(
        instances: Array[Instance],
        distanceFunction: DistanceFunction,
        pivotsAmount: Int
    ): Array[Instance] = {

        if(pivotsAmount >= candidatePivots.length){
            throw new Exception("The amount of pivots to select has to be less than the length of candidates set")
        }

        val candidates = ArrayBuffer.from(candidatePivots)
        val pivots = new ArrayBuffer[Instance]()

        while(pivots.length < pivotsAmount){

            val candidatesAndDistanceDistributionValues = candidates.map(candidate => {

                val pivotsAndCandidate = ArrayBuffer.from(pivots)
                pivotsAndCandidate.append(candidate)

                val meanDistanceDistribution = findPivotSetDistanceDistributionMean(pivotsAndCandidate.toArray, objectPairs, distanceFunction)

                (candidate, meanDistanceDistribution)
            }).toArray

            val bestCandidate = candidatesAndDistanceDistributionValues.maxBy(_._2)._1

            candidates.remove(candidates.indexOf(bestCandidate))
            pivots.append(bestCandidate)
        }

        pivots.toArray
    }

    /**
     *
     * @param set1 pivot set 1
     * @param set2 pivot set 2
     * @param objectPairs object pairs to test the distance distribution
     * @param distanceFunction distance function to be used
     * @return true if the first pivot set is better than the second one, false otherwise
     */
    def findBestPivotSet(set1: Array[Instance], set2: Array[Instance], objectPairs: Array[(Instance, Instance)], distanceFunction: DistanceFunction): Boolean ={

        val meanDistanceDistribution1 = findPivotSetDistanceDistributionMean(set1, objectPairs,distanceFunction)
        val meanDistanceDistribution2 = findPivotSetDistanceDistributionMean(set2, objectPairs,distanceFunction)

         meanDistanceDistribution1 > meanDistanceDistribution2
    }

    def findPivotSetDistanceDistributionMean(pivots: Array[Instance], objectPairs: Array[(Instance, Instance)], distanceFunction: DistanceFunction): Double = {
        val maxDistances = objectPairs.map(pair => {
            val (obj1, obj2) = pair
            val obj1Mapping = pivots.map(pivot => (pivot, distanceFunction(pivot.data, obj1.data)))
            val obj2Mapping = pivots.map(pivot => (pivot, distanceFunction(pivot.data, obj2.data)))
            val maxDistance = obj1Mapping.zip(obj2Mapping).map(tuple => {
                if(tuple._1._1.id != tuple._2._1.id){
                    throw new Exception("Pivots ids are not the same")
                }

                math.abs(tuple._1._2 - tuple._2._2)
            }).max

            maxDistance
        })

        maxDistances.sum / maxDistances.length.toDouble
    }
}
