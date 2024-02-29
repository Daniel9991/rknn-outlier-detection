package rknn_outlier_detection.search.small_data

import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor}
import rknn_outlier_detection.distance.DistanceFunctions

import scala.collection.mutable.ArrayBuffer

object ExhaustiveNeighbors {

  def findAllNeighbors(
    instances: Array[Instance],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): (Array[Array[KNeighbor]], Array[Array[Neighbor]]) = {

    val kNeighbors = instances.map(
      query => {
        val others = instances.zipWithIndex
          .filter(tuple => !tuple._1.attributes.sameElements(query.attributes))
        val otherInstances = others.map(tuple => tuple._1)
        val otherIndices = others.map(tuple => tuple._2)
        findQueryKNeighbors(query, otherInstances, otherIndices, k, distanceFunction)
      }
    )

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

    val kNeighbors = instances.map(
      query => {
        val others = instances.zipWithIndex
          .filter(tuple => !tuple._1.attributes.sameElements(query.attributes))
        val otherInstances = others.map(tuple => tuple._1)
        val otherIndices = others.map(tuple => tuple._2)
        findQueryKNeighbors(query, otherInstances, otherIndices, k, distanceFunction)
      }
    )

    kNeighbors
  }

  def findQueryKNeighbors(
    query: Instance,
    dataset: Array[Instance],
    indices: Array[Int],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): Array[KNeighbor] = {

    var kNeighbors = new ArrayBuffer[KNeighbor]()

    val (firstKInstances, remainingInstances) = dataset.splitAt(k)
    val (firstKIndices, remainingIndices) = indices.splitAt(k)

    firstKInstances.zip(firstKIndices).foreach(tuple => {
      val (instance, index) = tuple
      val distance = distanceFunction(query.attributes, instance.attributes)

      kNeighbors += new KNeighbor(instance.id, distance)
    })

    kNeighbors = kNeighbors.sortWith((neighbor1, neighbor2) =>
      neighbor1.distance < neighbor2.distance
    )

    remainingInstances.zip(remainingIndices).foreach(tuple => {
      val (instance, index) = tuple
      val distance = DistanceFunctions.euclidean(query.attributes, instance.attributes)

      if (kNeighbors.last.distance > distance) {

        val neighbor = new KNeighbor(instance.id, distance)
        insertNeighbor(kNeighbors, neighbor)
      }
    })

    kNeighbors.toArray
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
