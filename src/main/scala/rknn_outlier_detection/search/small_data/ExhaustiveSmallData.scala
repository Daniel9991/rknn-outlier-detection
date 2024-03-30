package rknn_outlier_detection.search.small_data

import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}
import rknn_outlier_detection.distance.DistanceFunctions
import rknn_outlier_detection.utils.Utils

import scala.collection.mutable.ArrayBuffer

object ExhaustiveSmallData {

  def findAllNeighbors(
    instances: Array[Instance],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): (Array[Array[KNeighbor]], Array[Array[Neighbor]]) = {

    val kNeighbors = findKNeighbors(instances, k, distanceFunction)

    val instancesWithKNeighbors = instances.zip(kNeighbors).map(tuple => {
      val (instance, kNeighborsBatch) = tuple

      val newInstance = new Instance(
        instance.id,
        instance.attributes,
        instance.classification
      )

      newInstance.kNeighbors = kNeighborsBatch

      newInstance
    })

    val reverseNeighbors = findReverseNeighbors(instancesWithKNeighbors)

    (kNeighbors, reverseNeighbors)
  }

  def findKNeighbors(
    instances: Array[Instance],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
 ): Array[Array[KNeighbor]] = {

    val kNeighbors = instances.map(query => findQueryKNeighbors(query, instances, k, distanceFunction))

    kNeighbors
  }

  def findQueryKNeighbors(
    query: Instance,
    dataset: Array[Instance],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): Array[KNeighbor] = {

    val kNeighbors = Array.fill[KNeighbor](k)(null)

    dataset.foreach(instance => {
        if(instance.id != query.id){
            val distance = distanceFunction(query.attributes, instance.attributes)

            if(kNeighbors.contains(null) || kNeighbors.last.distance > distance){
                Utils.addNewNeighbor(kNeighbors, new KNeighbor(instance.id, distance))
            }
        }
    })

    kNeighbors
  }

  def insertNeighbor(
    kNeighbors: ArrayBuffer[KNeighbor],
    newNeighbor: KNeighbor
  ): Unit = {

    var currentIndex = kNeighbors.length - 1
    kNeighbors(currentIndex) = newNeighbor

    while (newNeighbor != kNeighbors.head &&
      newNeighbor.distance < kNeighbors(currentIndex - 1).distance) {
      kNeighbors(currentIndex) = kNeighbors(currentIndex - 1)
      currentIndex -= 1
      kNeighbors(currentIndex) = newNeighbor
    }
  }

  def findReverseNeighbors(
    instancesWithKNeighbors: Array[Instance]
  ): Array[Array[Neighbor]] = {

    // Initialize empty arrays for reverse neighbors
    val reverseNeighbors = new Array[ArrayBuffer[Neighbor]](
      instancesWithKNeighbors.length
    )
    for (i <- instancesWithKNeighbors.indices) {
      reverseNeighbors(i) = new ArrayBuffer[Neighbor]()
    }


    instancesWithKNeighbors.zipWithIndex.foreach(tuple => {
      val (instance, index) = tuple

      instance.kNeighbors.foreach(kNeighbor => {
        val indexOfNeighbor = instancesWithKNeighbors.indexWhere(inst => inst.id == kNeighbor.id)
        reverseNeighbors(indexOfNeighbor) += new Neighbor(instance.id)
      })
    })

    reverseNeighbors.map(reverseNeighborsBatch => reverseNeighborsBatch.toArray)
  }
}
