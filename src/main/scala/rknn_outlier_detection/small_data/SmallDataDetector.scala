package rknn_outlier_detection.small_data

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.{Instance, RNeighbor}
import rknn_outlier_detection.small_data.detection.DetectionStrategy
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.collection.mutable
import scala.util.Random

class SmallDataDetector extends Serializable {

    def detect(instances: Seq[Instance], k: Int, pivotsAmount: Int, seed: Int, method: DetectionStrategy, distFn: DistanceFunction): Seq[(Int, Double)] ={
        val rand = new Random(seed)
        val pivots = rand.shuffle(instances).take(pivotsAmount)

        val pivotsWithInstance = instances.map(instance => {
            val pivotsAndDistances = pivots.map(pivot => (pivot, distFn(instance.data, pivot.data)))
            val closestPivot = pivotsAndDistances.reduce((pivot1, pivot2) => if(pivot2._2 < pivot1._2) pivot2 else pivot1)
            (closestPivot._1, instance)
        })

        println("Found pivots per instance")
        var counter = 1
        val exhaustiveSmallData = new ExhaustiveSmallData()
        val groupedInstances = pivotsWithInstance.groupMap(_._1)(_._2).values
        val outlierDegrees = groupedInstances.flatMap(group => {
            println(s"Processing group $counter out of $pivotsAmount with ${group.length} instances")
            val allKNeighbors = group.map(instance => (instance.id, exhaustiveSmallData.findQueryKNeighbors(instance, group.toArray, k, distFn)))
            val filteredKNeighbors = allKNeighbors.map{case (id, neighbors) => (id, neighbors.filter(n => n != null))}
            val rNeighbors = filteredKNeighbors.flatMap{case (instanceId, kNeighbors) =>
                kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
            }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray
            .map(tuple => (tuple._1, tuple._2.toArray))

            val elementsSet = mutable.HashSet.from(group.map(_.id))
            rNeighbors.foreach(pair => elementsSet.remove(pair._1))
            val elementsWithoutRNeighbors = elementsSet.map(id => (id, Array.empty[RNeighbor])).toArray

            val outlierDegrees = method.scoreInstances(rNeighbors.concat(elementsWithoutRNeighbors))
            counter += 1
            outlierDegrees
        })

        outlierDegrees.toSeq
    }
}
