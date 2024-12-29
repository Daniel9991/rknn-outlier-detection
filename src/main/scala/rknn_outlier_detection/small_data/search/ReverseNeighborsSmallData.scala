package rknn_outlier_detection.small_data.search

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

import scala.collection.mutable.ArrayBuffer

object ReverseNeighborsSmallData {

    def findReverseNeighbors(allKNeighbors: Array[Array[KNeighbor]]): Array[Array[RNeighbor]] = {
        // The String in the tuple happens to be the index of that instance

        val reverseNeighbors = new Array[ArrayBuffer[RNeighbor]](
            allKNeighbors.length
        )
        for (i <- allKNeighbors.indices) {
            reverseNeighbors(i) = new ArrayBuffer[RNeighbor]()
        }

        allKNeighbors.zipWithIndex.foreach(tuple => {
            val (kNeighbors, instanceIndex) = tuple

            kNeighbors.zipWithIndex.foreach(subTuple => {
                val (kNeighbor, kNeighborIndex) = subTuple
                reverseNeighbors(kNeighbor.id) += new RNeighbor(instanceIndex, kNeighborIndex)
            })
        })

        reverseNeighbors.map(_.toArray)
    }
}
