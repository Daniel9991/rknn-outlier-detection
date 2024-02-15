package rknn_outlier_detection.search.small_data

import rknn_outlier_detection.custom_objects.{KNeighbor, Neighbor}
import rknn_outlier_detection.distance.DistanceFunctions

import scala.collection.mutable.ArrayBuffer

object ExhaustiveNeighbors {

  def findAllNeighbors(
    instances: Array[Array[Double]],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): (Array[Array[KNeighbor]], Array[Array[Neighbor]]) = {

    val kNeighbors = instances.map(
      query => {
        val others = instances.zipWithIndex
          .filter(tuple => !tuple._1.sameElements(query))
        val otherInstances = others.map(tuple => tuple._1)
        val otherIndices = others.map(tuple => tuple._2)
        findQueryKNeighbors(query, otherInstances, otherIndices, k, distanceFunction)
      }
    )

    val reverseNeighbors = findReverseNeighbors(kNeighbors)

    (kNeighbors, reverseNeighbors)
  }

  def findKNeighbors(
    instancesAttributes: Array[Array[Double]],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
 ): Array[Array[KNeighbor]] = {

    val kNeighbors = instancesAttributes.map(
      query => {
        val others = instancesAttributes.zipWithIndex
          .filter(tuple => !tuple._1.sameElements(query))
        val otherInstances = others.map(tuple => tuple._1)
        val otherIndices = others.map(tuple => tuple._2)
        findQueryKNeighbors(query, otherInstances, otherIndices, k, distanceFunction)
      }
    )

    kNeighbors
  }

  def findQueryKNeighbors(
    query: Array[Double],
    dataset: Array[Array[Double]],
    indices: Array[Int],
    k: Int,
    distanceFunction: (Array[Double], Array[Double]) => Double
  ): Array[KNeighbor] = {

    var kNeighbors = new ArrayBuffer[KNeighbor]()

    val (firstKInstances, remainingInstances) = dataset.splitAt(k)
    val (firstKIndices, remainingIndices) = indices.splitAt(k)

    firstKInstances.zip(firstKIndices).foreach(tuple => {
      val (instance, index) = tuple
      val distance = distanceFunction(query, instance)

      kNeighbors += new KNeighbor(index.toString, distance)
    })

    kNeighbors = kNeighbors.sortWith((neighbor1, neighbor2) =>
      neighbor1.distance < neighbor2.distance
    )

    remainingInstances.zip(remainingIndices).foreach(tuple => {
      val (instance, index) = tuple
      val distance = DistanceFunctions.euclidean(query, instance)

      if (kNeighbors.last.distance > distance) {

        val neighbor = new KNeighbor(index.toString, distance)
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
    instancesKNeighbors: Array[Array[KNeighbor]]
  ): Array[Array[Neighbor]] = {

    val reverseNeighbors = new Array[ArrayBuffer[Neighbor]](
      instancesKNeighbors.length
    )

    for (i <- instancesKNeighbors.indices) {
      reverseNeighbors(i) = new ArrayBuffer[Neighbor]()
    }

    instancesKNeighbors.zipWithIndex.foreach(tuple => {
      val (kNeighborsBatch, index) = tuple

      kNeighborsBatch.foreach(kNeighbor =>
        reverseNeighbors(kNeighbor.id.toInt) += new Neighbor(index.toString)
      )
    })

    reverseNeighbors.map(reverseNeighborsBatch => reverseNeighborsBatch.toArray)
  }
}
