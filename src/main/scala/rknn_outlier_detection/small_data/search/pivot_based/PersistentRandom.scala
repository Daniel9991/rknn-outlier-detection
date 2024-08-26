package rknn_outlier_detection.small_data.search.pivot_based
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class PersistentRandom(retries: Int, objectSet: Array[Instance]) extends PivotSelector with Serializable {

    def findPivotsCoefficientOfVariation(instances: Array[Instance], pivots: Array[Instance], distanceFunction: DistanceFunction): Double = {

        val cells = mutable.Map[String, ArrayBuffer[Instance]]()

        pivots.foreach(pivot => {
//            println(s"Creating cell for pivot ${pivot.id}")
            cells(pivot.id) = new ArrayBuffer[Instance]()
        })

        instances.foreach(instance => {
//            println(s"Adding instance ${instance.id} to closest cell")
            val closestPivotId = pivots.map(pivot => (pivot.id, distanceFunction(pivot.data, instance.data))).minBy(_._2)._1
            cells(closestPivotId).addOne(instance)
        })

        val cellsLengths = cells.map(_._2.length).toArray
        val mean = cellsLengths.sum.toDouble / cellsLengths.length.toDouble
        val std = math.sqrt(cellsLengths.map(length => math.pow(length - mean, 2)).sum / cellsLengths.length)
        std / mean

    }

    override def findPivots(instances: Array[Instance], distanceFunction: DistanceFunction, pivotsAmount: Int): Array[Instance] = {

        var bestPivots: Array[Instance] = null
        var bestCoefficientOfVariation = Double.PositiveInfinity

        for(i <- 0 until retries){
//            println(s"Processing ${i}")
            val currentPivots = Random.shuffle(objectSet.toSeq).take(pivotsAmount).toArray
            val currentCoefficientOfVariation = findPivotsCoefficientOfVariation(instances, currentPivots, distanceFunction)

            if(currentCoefficientOfVariation < bestCoefficientOfVariation){
                bestPivots = currentPivots
                bestCoefficientOfVariation = currentCoefficientOfVariation
            }
        }

        bestPivots
    }
}
