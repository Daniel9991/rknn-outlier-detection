package rknn_outlier_detection.big_data.search.pivot_based
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.shared.custom_objects.Instance

import scala.util.Random

class RandomPivotSelector(val seed: Long = Random.nextLong()) extends PivotSelectionStrategy {
    override def selectPivots(instances: RDD[Instance], pivotsAmount: Int): Array[Instance] = {
        instances.takeSample(withReplacement = false, pivotsAmount, seed)
    }
}
