package rknn_outlier_detection.small_data.search

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, Neighbor}

import scala.collection.mutable.ArrayBuffer

object ReverseNeighborsSmallData {
    def findReverseNeighborsFromInstance(
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

            instance.kNeighbors.zipWithIndex.foreach(subTuple => {
                val (kNeighbor, index) = subTuple
                val indexOfNeighbor = instancesWithKNeighbors.indexWhere(inst => inst.id == kNeighbor.id)
                reverseNeighbors(indexOfNeighbor) += new Neighbor(instance.id, index)
            })
        })

        reverseNeighbors.map(reverseNeighborsBatch => reverseNeighborsBatch.toArray)
    }

    def findReverseNeighbors(allKNeighbors: Array[Array[KNeighbor]]): Array[Array[Neighbor]] = {
        // The String in the tuple happens to be the index of that instance

        val reverseNeighbors = new Array[ArrayBuffer[Neighbor]](
            allKNeighbors.length
        )
        for (i <- allKNeighbors.indices) {
            reverseNeighbors(i) = new ArrayBuffer[Neighbor]()
        }

        allKNeighbors.zipWithIndex.foreach(tuple => {
            val (kNeighbors, instanceIndex) = tuple

            kNeighbors.zipWithIndex.foreach(subTuple => {
                val (kNeighbor, kNeighborIndex) = subTuple
                reverseNeighbors(kNeighbor.id.toInt) += new Neighbor(instanceIndex.toString, kNeighborIndex)
            })
        })

        reverseNeighbors.map(_.toArray)
    }
}
