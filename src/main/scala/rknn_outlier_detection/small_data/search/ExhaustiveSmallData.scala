package rknn_outlier_detection.small_data.search

import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.Utils

import scala.collection.mutable.ArrayBuffer

class ExhaustiveSmallData[A] extends KNNSearchStrategy[A] {

    def findKNeighbors(
        instances: Array[Instance[A]],
        k: Int,
        distanceFunction: DistanceFunction[A]
    ): Array[Array[KNeighbor]] = {

        val kNeighbors = instances.map(query => findQueryKNeighbors(query, instances, k, distanceFunction))

        kNeighbors
    }

    def findQueryKNeighbors(
        query: Instance[A],
        dataset: Array[Instance[A]],
        k: Int,
        distanceFunction: DistanceFunction[A]
    ): Array[KNeighbor] = {

        val kNeighbors = Array.fill[KNeighbor](k)(null)

        dataset.foreach(instance => {
            if (instance.id != query.id) {
                val distance = distanceFunction(query.data, instance.data)

                if (kNeighbors.contains(null) || kNeighbors.last.distance > distance) {
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
}
