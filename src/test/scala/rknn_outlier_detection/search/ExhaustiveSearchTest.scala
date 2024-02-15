package rknn_outlier_detection.search

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}

class ExhaustiveSearchTest extends AnyFunSuite {

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

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))
    val searchStrategy: ExhaustiveSearch.type = ExhaustiveSearch

    val i1 = new Instance("1", Array(1.0, 1.0), "")
    val i2 = new Instance("2", Array(2.0, 2.0), "")
    val i3 = new Instance("3", Array(3.0, 3.0), "")
    val i4 = new Instance("4", Array(4.0, 4.0), "")
    val i5 = new Instance("5", Array(5.0, 5.0), "")
    val i6 = new Instance("6", Array(1.9, 1.9), "")
    val i7 = new Instance("7", Array(2.2, 2.2), "")

    test("Empty RDD"){
        val testingData = sc.parallelize(Seq[Instance]())
        val result = searchStrategy.findKNeighbors(testingData, 1, sc)
        assert(result.isEmpty())
    }

    test("k less than 1"){
        // TODO: Implement this test
        // Create UnacceptableKValue and throw it in findKNeighbors implementations
        // if k <= 0 or k >= n
    }

    test("General knn and rknn"){
        val k = 3
        val testingData = sc.parallelize(Seq(i1, i2, i3, i4, i5))

        val kNeighborsRDD = searchStrategy.findKNeighbors(testingData, k, sc)
        val kNeighbors = kNeighborsRDD.collect()

        val rNeighborsRDD = ReverseKNNSearch.findReverseNeighbors(kNeighborsRDD)
        val rNeighbors = rNeighborsRDD.collect()

        assert(kNeighbors.forall(pair => pair._2.length == k))

        val sortedKNeighbors = kNeighbors.sortWith((a, b) => a._1 < b._1)
        val sortedRNeighbors = rNeighbors.sortWith((a, b) => a._1 < b._1)

        // instance1
        assert(arraysContainSameIds(sortedKNeighbors(0)._2.map(_.id), Array("2", "3", "4")))
        assert(sortedRNeighbors(0)._2.map(_.id).contains("2"))

        // instance2
        assert(arraysContainSameIds(sortedKNeighbors(1)._2.map(_.id), Array("1", "3", "4")))
        assert(arraysContainSameIds(sortedRNeighbors(1)._2.map(_.id), Array("1", "3", "4", "5")))

        // instance3
        assert(
            arraysContainSameIds(sortedKNeighbors(2)._2.map(_.id), Array("2", "4", "1")) ||
                arraysContainSameIds(sortedKNeighbors(2)._2.map(_.id), Array("2", "4", "5"))
        )
        assert(arraysContainSameIds(sortedRNeighbors(2)._2.map(_.id), Array("1", "2", "4", "5")))

        // instance4
        assert(arraysContainSameIds(sortedKNeighbors(3)._2.map(_.id), Array("2", "3", "5")))
        assert(arraysContainSameIds(sortedRNeighbors(3)._2.map(_.id), Array("1", "2", "3", "5")))

        // instance5
        assert(arraysContainSameIds(sortedKNeighbors(4)._2.map(_.id), Array("2", "3", "4")))
        assert(sortedRNeighbors(4)._2.map(_.id).contains("4"))

        // instance1 or instance5 got instance3 as reverseNeighbors
        assert(
            (sortedRNeighbors(0)._2.length == 1  && sortedRNeighbors(4)._2.length == 2 && sortedRNeighbors(4)._2.map(_.id).contains("3")) ||
            (sortedRNeighbors(0)._2.length == 2  && sortedRNeighbors(4)._2.length == 1 && sortedRNeighbors(0)._2.map(_.id).contains("3"))
        )
    }

    test("One instance doesn't have rnn"){
        val k = 2
        val testingData = sc.parallelize(Seq(i1, i6, i2, i7))

        val kNeighborsRDD = searchStrategy.findKNeighbors(testingData, k, sc)

        val rNeighborsRDD = ReverseKNNSearch.findReverseNeighbors(kNeighborsRDD)
        val rNeighbors = rNeighborsRDD.collect()

        val sortedRNeighbors = rNeighbors.sortWith((a, b) => a._1 < b._1)

        // instance1
        assert(sortedRNeighbors(0)._2.isEmpty)

        // instance2
        assert(arraysContainSameIds(sortedRNeighbors(1)._2.map(_.id), Array("6", "7", "1")))

        // instance6
        assert(arraysContainSameIds(sortedRNeighbors(2)._2.map(_.id), Array("2", "7", "1")))

        // instance7
        assert(arraysContainSameIds(sortedRNeighbors(3)._2.map(_.id), Array("6", "2")))
    }
}