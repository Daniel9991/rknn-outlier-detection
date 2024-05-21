package rknn_outlier_detection.small_data.search

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

import scala.collection.mutable.ArrayBuffer

object ReverseNeighborsSmallData {
    def findReverseNeighborsFromInstance(
        instancesWithKNeighbors: Array[Instance]
    ): Array[Array[RNeighbor]] = {

        // Initialize empty arrays for reverse neighbors
        val reverseNeighbors = new Array[ArrayBuffer[RNeighbor]](
            instancesWithKNeighbors.length
        )
        for (i <- instancesWithKNeighbors.indices) {
            reverseNeighbors(i) = new ArrayBuffer[RNeighbor]()
        }


        instancesWithKNeighbors.zipWithIndex.foreach(tuple => {
            val (instance, index) = tuple

            instance.kNeighbors.zipWithIndex.foreach(subTuple => {
                val (kNeighbor, index) = subTuple
                val indexOfNeighbor = instancesWithKNeighbors.indexWhere(inst => inst.id == kNeighbor.id)
                reverseNeighbors(indexOfNeighbor) += new RNeighbor(instance.id, index)
            })
        })

        reverseNeighbors.map(reverseNeighborsBatch => reverseNeighborsBatch.toArray)
    }

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
                reverseNeighbors(kNeighbor.id.toInt) += new RNeighbor(instanceIndex.toString, kNeighborIndex)
            })
        })

        reverseNeighbors.map(_.toArray)
    }
}
