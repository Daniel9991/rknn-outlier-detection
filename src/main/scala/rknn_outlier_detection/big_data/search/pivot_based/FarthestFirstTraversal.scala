package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.collection.mutable.ArrayBuffer

class FarthestFirstTraversal(seed: Int = 6745) {
    def findPivots(_objectSet: RDD[Instance], pivotsAmount: Int, distanceFunction: DistanceFunction): Array[Instance] = {

        val initialPivot = _objectSet.takeSample(withReplacement = false, seed=seed, num=1)(0)
        var objectSet = _objectSet.filter(o => o.id != initialPivot.id)
        val pivotsBuffer = ArrayBuffer(initialPivot)

        while(pivotsBuffer.length < pivotsAmount){
            val nextPivot =  objectSet.map(o => (o, pivotsBuffer.map(pivot => distanceFunction(pivot.data, o.data)).min)).reduce{case (t1, t2) => if(t2._2 > t1._2) t2 else t1}._1
            pivotsBuffer += nextPivot
            objectSet = objectSet.filter(o => o.id != nextPivot.id)
        }

        pivotsBuffer.toArray
    }
}
