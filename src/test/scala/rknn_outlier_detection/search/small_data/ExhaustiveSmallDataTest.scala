package rknn_outlier_detection.search.small_data

import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.DistanceFunctions

class ExhaustiveSmallDataTest extends AnyFunSuite {

    val i1 = new Instance("1", Array(1.0, 1.0), "")
    val i2 = new Instance("2", Array(2.0, 2.0), "")
    val i3 = new Instance("3", Array(3.0, 3.0), "")
    val i4 = new Instance("4", Array(4.0, 4.0), "")
    val i5 = new Instance("5", Array(5.0, 5.0), "")
    val i6 = new Instance("6", Array(1.9, 1.9), "")
    val i7 = new Instance("7", Array(2.2, 2.2), "")

    test("Empty RDD"){
        val testingData = Array[Instance]()
        val result = ExhaustiveNeighbors.findKNeighbors(testingData, 1, DistanceFunctions.euclidean)
        assert(result.isEmpty)
    }

    // test("k less than 1"){
        // TODO: Implement this test
        // Create UnacceptableKValue and throw it in findKNeighbors implementations
        // if k <= 0 or k >= n
    // }

    test("General knn and rknn"){
        val k = 3
        val testingData = Array(i1, i2, i3, i4, i5)

        val (kNeighbors, rNeighbors) = ExhaustiveNeighbors.findAllNeighbors(testingData, k, DistanceFunctions.euclidean)

        assert(kNeighbors.forall(neighbor => neighbor.length == k))

        // instance1
        // println(kNeighbors.map(neighbors => neighbors.map(_.id).mkString("[", ", ", "]")).mkString("[\n\t", "\n\t", "\n]"))
        assert(arraysContainSameIds(kNeighbors(0).map(_.id), Array("2", "3", "4")))
        assert(rNeighbors(0).map(_.id).contains("2"))

        // instance2
        assert(arraysContainSameIds(kNeighbors(1).map(_.id), Array("1", "3", "4")))
        assert(arraysContainSameIds(rNeighbors(1).map(_.id), Array("1", "3", "4", "5")))

        // instance3
        assert(
            arraysContainSameIds(kNeighbors(2).map(_.id), Array("2", "4", "1")) ||
            arraysContainSameIds(kNeighbors(2).map(_.id), Array("2", "4", "5"))
        )
        assert(arraysContainSameIds(rNeighbors(2).map(_.id), Array("1", "2", "4", "5")))

        // instance4
        assert(arraysContainSameIds(kNeighbors(3).map(_.id), Array("2", "3", "5")))
        assert(arraysContainSameIds(rNeighbors(3).map(_.id), Array("1", "2", "3", "5")))

        // instance5
        assert(arraysContainSameIds(kNeighbors(4).map(_.id), Array("2", "3", "4")))
        assert(rNeighbors(4).map(_.id).contains("4"))

        // instance1 or instance5 got instance3 as reverseNeighbors
        assert(
            (rNeighbors(0).length == 1 && rNeighbors(4).length == 2 && rNeighbors(4).map(_.id).contains("3")) ||
            (rNeighbors(0).length == 2  && rNeighbors(4).length == 1 && rNeighbors(0).map(_.id).contains("3"))
        )
    }

    test("One instance doesn't have rnn"){
        val k = 2
        val testingData = Array(i1, i6, i2, i7)

        val (_, rNeighbors) = ExhaustiveNeighbors.findAllNeighbors(testingData, k, DistanceFunctions.euclidean)

        // instance1
        assert(rNeighbors(0).isEmpty)

        // instance6
        assert(arraysContainSameIds(rNeighbors(1).map(_.id), Array("1", "2", "7")))

        // instance2
        assert(arraysContainSameIds(rNeighbors(2).map(_.id), Array("1", "6", "7")))

        // instance7
        assert(arraysContainSameIds(rNeighbors(3).map(_.id), Array("6", "2")))
    }

    def arraysContainSameNeighbors(arr1: Array[KNeighbor], arr2: Array[KNeighbor]): Boolean = {
        if(arr1.length != arr2.length) return false
        if(arr1.isEmpty) return false

        var sameElements = true
        val arr1Ids = arr1.map(_.id)
        val arr2Ids = arr2.map(_.id)
        var index = 0

        while(sameElements && index < arr1Ids.length){
            if(!arr2Ids.contains(arr1Ids(index))) sameElements = false
            index += 1
        }

        if(sameElements) return true

        val arr1SortedDist = arr1.map(_.distance).sorted
        val arr2SortedDist = arr2.map(_.distance).sorted

        arr1SortedDist.zip(arr2SortedDist).forall(pair => pair._1 == pair._2)
    }

    def arraysContainSameIds(arr1: Array[String], arr2: Array[String]): Boolean = {
        if(arr1.length != arr2.length || arr1.isEmpty) return false

        val sortedArr1 = arr1.sorted
        val sortedArr2 = arr2.sorted

        sortedArr1.sameElements(sortedArr2)
    }
}
