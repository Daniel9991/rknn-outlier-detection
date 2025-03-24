package rknn_outlier_detection

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.classification.ClassificationStrategy
import rknn_outlier_detection.big_data.partitioners.PivotsPartitioner
import rknn_outlier_detection.big_data.search.pivot_based.PivotSelectionStrategy
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}
import rknn_outlier_detection.small_data.detection.DetectionStrategy
import rknn_outlier_detection.small_data.search.KNNSearchStrategy

import scala.collection.mutable

class ByPartitionDetector(
  val pivotSelectionStrategy: PivotSelectionStrategy,
  val searchStrategy: KNNSearchStrategy,
  val detectionStrategy: DetectionStrategy,
  val classificationStrategy: ClassificationStrategy,
  val sc: SparkContext
) {

    def detectOutliers(
        instances: RDD[Instance],
        k: Int,
        pivotsAmount: Int,
        distanceFunction: DistanceFunction,
        normalLabel: String,
        outlierLabel: String
    ): RDD[(Int, Double, String)] = {

        val scores = scoreInstances(instances, k, pivotsAmount, distanceFunction)
        val classificationAndScores = classificationStrategy.classify(scores, normalLabel, outlierLabel)
        classificationAndScores
    }

    def scoreInstances(instances: RDD[Instance], k: Int, pivotsAmount: Int, distanceFunction: DistanceFunction): RDD[(Int, Double)] = {

        val selectedPivots = pivotSelectionStrategy.selectPivots(instances, pivotsAmount)
        val pivots = sc.broadcast(selectedPivots)

        if(selectedPivots.length != pivotsAmount)
            throw new Exception("There was a different amount of sampled pivots than required")

        // Create cells
        val cells = instances.map(instance => {
            val closestPivot = pivots.value
                .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

            (closestPivot._1, instance)
        }).partitionBy(new PivotsPartitioner(pivotsAmount, selectedPivots.map(_.id))).persist()

        val result = cells.mapPartitions(iter => {
            // All elements from the same partition should belong to the same pivot
            val elements = iter.toArray.map{case (pivot, instance) => instance}
            val allKNeighbors = searchStrategy.findKNeighbors(elements, k, distanceFunction)
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
}
