package rknn_outlier_detection.search.small_data

import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.DistanceFunctions

class LAESASmallDataTest extends AnyFunSuite {

    val i1 = new Instance("1", Array(1.0, 1.0), "")
    val i2 = new Instance("2", Array(2.0, 2.0), "")
    val i3 = new Instance("3", Array(3.0, 3.0), "")
    val i4 = new Instance("4", Array(4.0, 4.0), "")
    val i5 = new Instance("5", Array(5.0, 5.0), "")
    val i6 = new Instance("6", Array(1.9, 1.9), "")
    val i7 = new Instance("7", Array(2.2, 2.2), "")

    val LAESAConfig = new LAESA(1)

    test("Empty result"){
        val testingData = Array[Instance]()
        val result = LAESAConfig.findAllKNeighbors(testingData)
        assert(result.isEmpty)
    }

    // test("k less than 1"){
    // TODO: Implement this test
    // Create UnacceptableKValue and throw it in findKNeighbors implementations
    // if k <= 0 or k >= n
    // }

    test("General knn"){
        val k = 3
        val testingData = Array(i1, i2, i3, i4, i5)

        val kNeighbors = LAESAConfig.findAllKNeighbors(testingData)

        // instance 1
        println(s"Neighbor of instance 1: ${kNeighbors(0).id}, ${kNeighbors(0).distance}")
        assert(kNeighbors(0).id == "2" || kNeighbors(0).id == "3")

        // instance 2
        println(s"Neighbor of instance 2: ${kNeighbors(1).id}, ${kNeighbors(1).distance}")
        assert(kNeighbors(1).id == "1" || kNeighbors(1).id == "3")

        // instance 3
        println(s"Neighbor of instance 3: ${kNeighbors(2).id}, ${kNeighbors(2).distance}")
        assert(kNeighbors(2).id == "2" || kNeighbors(2).id == "4")

        // instance 4
        println(s"Neighbor of instance 4: ${kNeighbors(3).id}, ${kNeighbors(3).distance}")
        assert(kNeighbors(3).id == "3" || kNeighbors(3).id == "5")

        // instance 5
        println(s"Neighbor of instance 5: ${kNeighbors(4).id}, ${kNeighbors(4).distance}")
        assert(kNeighbors(4).id == "4")
    }

//    test("One instance doesn't have rnn"){
//        val k = 2
//        val testingData = Array(i1, i6, i2, i7)
//
//        val (_, rNeighbors) = ExhaustiveNeighbors.findAllNeighbors(testingData, k, DistanceFunctions.euclidean)
//
//        // instance1
//        assert(rNeighbors(0).isEmpty)
//
//        // instance6
//        assert(arraysContainSameIds(rNeighbors(1).map(_.id), Array("1", "2", "7")))
//
//        // instance2
//        assert(arraysContainSameIds(rNeighbors(2).map(_.id), Array("1", "6", "7")))
//
//        // instance7
//        assert(arraysContainSameIds(rNeighbors(3).map(_.id), Array("6", "2")))
//    }

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
