package rknn_outlier_detection.big_data.search

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.{DistanceFunction, euclidean}
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.big_data.search.pivot_based.{PkNN, WIPPkNN}
import rknn_outlier_detection.exceptions.{IncorrectKValueException, InsufficientInstancesException}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

class PkNNTest extends AnyFunSuite {

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

    val i1 = new Instance("1", Array(1.0, 1.0))
    val i2 = new Instance("2", Array(2.0, 2.0))
    val i3 = new Instance("3", Array(3.0, 3.0))
    val i4 = new Instance("4", Array(5.0, 5.0))
    val i5 = new Instance("5", Array(1.9, 1.6))
    val i6 = new Instance("6", Array(2.2, 2.4))
    val i7 = new Instance("7", Array(6.4, 7.7))

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Sparking2").set("spark.default.parallelism", "16"))
    val searchStrategy = new PkNN(Array(i1, i4), 1000)
    val distFun: DistanceFunction = euclidean



//    test("k less than 1"){
//        val testingData = sc.parallelize(Seq[Instance](i1, i2))
//        assertThrows[IncorrectKValueException]{
//            searchStrategy.findKNeighbors(testingData, 0, distFun, sc)
//        }
//        assertThrows[IncorrectKValueException]{
//            searchStrategy.findKNeighbors(testingData, -1, distFun, sc)
//        }
//    }
//
//    test("instances amount is less than 2"){
//        val testingData1 = sc.parallelize(Seq[Instance]())
//        val testingData2 = sc.parallelize(Seq[Instance](i1))
//        assertThrows[InsufficientInstancesException]{
//            searchStrategy.findKNeighbors(testingData1, 1, distFun, sc)
//        }
//        assertThrows[InsufficientInstancesException]{
//            searchStrategy.findKNeighbors(testingData2, 1, distFun, sc)
//        }
//    }
//
//    test("k value is instances length"){
//        val testingData = sc.parallelize(Seq[Instance](i1, i2, i3, i4))
//        assertThrows[IncorrectKValueException]{
//            searchStrategy.findKNeighbors(testingData, 4, distFun, sc)
//        }
//        assertThrows[IncorrectKValueException]{
//            searchStrategy.findKNeighbors(testingData, 5, distFun, sc)
//        }
//    }
//
    test("knn results"){
        val k = 6
        val testingData = sc.parallelize(Seq(i1, i2, i3, i4, i5, i6, i7), 2)

        val kNeighborsRDD = searchStrategy.findKNeighborsExperiment(testingData, k, distFun, sc)
        val kNeighbors = kNeighborsRDD.collect()

        println(kNeighbors.map(t => s"${t._1}: ${t._2.map(n => s"${n.id} - ${n.distance}").mkString(", ")}").mkString("\n"))

        assert(kNeighbors.forall(pair => pair._2.length == k))

        val sortedKNeighbors = kNeighbors.sortWith((a, b) => a._1 < b._1)

        // instance1
        val instance1NeighborsIds = sortedKNeighbors(0)._2.map(_.id)
        println(s"Los neighbors de instance 1 son: ${instance1NeighborsIds.mkString("Array(", ", ", ")")}")
        assert(instance1NeighborsIds.sameElements(Array("5", "2", "6", "3", "4", "7")))

        // instance2
        val instance2NeighborsIds = sortedKNeighbors(1)._2.map(_.id)
        assert(
            instance2NeighborsIds.sameElements(Array("5", "6", "1", "3", "4", "7")) ||
                instance2NeighborsIds.sameElements(Array("5", "6", "3", "1", "4", "7"))
        )

        // instance3
        val instance3NeighborsIds = sortedKNeighbors(2)._2.map(_.id)
        assert(
            instance3NeighborsIds.sameElements(Array("6", "2", "5", "1", "4", "7")) ||
                instance3NeighborsIds.sameElements(Array("6", "2", "5", "4", "1", "7"))
        )

        // instance4
        val instance4NeighborsIds = sortedKNeighbors(3)._2.map(_.id)
        assert(instance4NeighborsIds.sameElements(Array("3", "7", "6", "2", "5", "1")))

        // instance5
        val instance5NeighborsIds = sortedKNeighbors(4)._2.map(_.id)
        assert(instance5NeighborsIds.sameElements(Array("2", "6", "1", "3", "4", "7")))

        // instance6
        val instance6NeighborsIds = sortedKNeighbors(5)._2.map(_.id)
        assert(instance6NeighborsIds.sameElements(Array("2", "5", "3", "1", "4", "7")))

        // instance7
        val instance7NeighborsIds = sortedKNeighbors(6)._2.map(_.id)
        assert(instance7NeighborsIds.sameElements(Array("4", "3", "6", "2", "5", "1")))

    }

    test("(Iris) General knn and rknn"){
        val k = 10

        // Read rows from csv file and convert them to Instance objects
        val rawData = ReaderWriter.readCSV("datasets/iris-synthetic-2.csv", hasHeader=false)
        val baseInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            new Instance(index.toString, attributes)
        })

        // Getting kNeighbors from ExhaustiveSearch small data
        val smallKNeighbors = new ExhaustiveSmallData().findKNeighbors(baseInstances, k, euclidean)

        // Getting kNeighbors from ExhaustiveSearch big data
        val rdd = sc.parallelize(baseInstances.toSeq, 2)
        val pivots = rdd.takeSample(withReplacement = false, 2, 543)
        val kNeighborsRDD = new PkNN(pivots, 1000).findKNeighborsExperiment(rdd, k, distFun, sc)
        val bigKNeighbors = kNeighborsRDD
            .collect()
            .map(tuple => (tuple._1.toInt, tuple._2))
            .sortWith((t1, t2) => t1._1 < t2._1)
            .map(_._2)

        val mixedSmallAndBigKNeighbors = smallKNeighbors.zip(bigKNeighbors)
        var count = 0
        assert(smallKNeighbors.length == bigKNeighbors.length)
        assert(mixedSmallAndBigKNeighbors.forall(tuple => {
            val (small, big) = tuple
            if(!arraysContainSameNeighbors(small, big)){
                println(s"It happens for number $count")
                println(small.map(n => s"\n${n.id}-${n.distance}").mkString("Array(", ", ", ")"))
                println(big.map(n => s"\n${n.id}-${n.distance}").mkString("Array(", ", ", ")"))
            }
            count += 1
            arraysContainSameNeighbors(small, big)
        }))
    }
}
