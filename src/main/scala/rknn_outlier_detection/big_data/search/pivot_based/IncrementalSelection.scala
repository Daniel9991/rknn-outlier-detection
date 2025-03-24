package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

class IncrementalSelection {

    def findPivots(initialCandidates: RDD[Instance], objectPairs: RDD[(Instance, Instance)], pivotsAmount: Int, distanceFunction: DistanceFunction, sc: SparkContext): Array[Instance] = {

        def accumulateValuesForMean(tuple1: (Double, Int), tuple2: (Double, Int)): (Double, Int) = {
            (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
        }

        def calculateMean(tuple: (Double, Int)): Double = tuple._1 / tuple._2

        val pivots = scala.collection.mutable.ArrayBuffer.empty[Instance]
        var candidates = initialCandidates

        while(pivots.length < pivotsAmount){
            val candidateToObjectPairs = candidates.cartesian(objectPairs).coalesce(sc.defaultParallelism)
            val values = candidateToObjectPairs.map{case (candidate, (obj1, obj2)) =>
                val candidateAndPivots = pivots.clone() += candidate
                val maxValue = candidateAndPivots.map(pivot =>
                    scala.math.abs(
                        distanceFunction(pivot.data, obj1.data) - distanceFunction(pivot.data, obj2.data)
                    )
                ).max
                (candidate, (maxValue, 1))
            }
            val meanValues = values.reduceByKey(accumulateValuesForMean).mapValues(calculateMean)
            val bestCandidate = meanValues.reduce{case (tuple1, tuple2) => if(tuple2._2 > tuple1._2) tuple2 else tuple1}._1
            pivots += bestCandidate
            candidates = candidates.filter(candidate => candidate.id != bestCandidate.id)
        }

        pivots.toArray
    }
}
