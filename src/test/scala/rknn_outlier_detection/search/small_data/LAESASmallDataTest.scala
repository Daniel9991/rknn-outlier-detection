package rknn_outlier_detection.search.small_data

import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.DistanceFunctions
import rknn_outlier_detection.utils.ReaderWriter

class LAESASmallDataTest extends AnyFunSuite {

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

    val i1 = new Instance("1", Array(1.0, 1.0), "")
    val i2 = new Instance("2", Array(2.0, 2.0), "")
    val i3 = new Instance("3", Array(3.0, 3.0), "")
    val i4 = new Instance("4", Array(4.0, 4.0), "")
    val i5 = new Instance("5", Array(5.0, 5.0), "")
    val i6 = new Instance("6", Array(1.9, 1.9), "")
    val i7 = new Instance("7", Array(2.2, 2.2), "")

    test("Empty result"){
        val LAESAConfig = new LAESA(1)
        val testingData = Array[Instance]()
        val result = LAESAConfig.findAllKNeighbors(testingData, 3)
        assert(result.isEmpty)
    }

    // test("k less than 1"){
    // TODO: Implement this test
    // Create UnacceptableKValue and throw it in findKNeighbors implementations
    // if k <= 0 or k >= n
    // }

    test("(Dummy) General knn"){
        val LAESAConfig = new LAESA(1)
        val k = 3
        val testingData = Array(i1, i2, i3, i4, i5)

        val kNeighbors = LAESAConfig.findAllKNeighbors(testingData, k)

        // instance 1
//        println(kNeighbors.map(neighbors => neighbors.map(n => if(n != null)  n.id else null).mkString("[", ", ", "]")).mkString("[\n\t", "\n\t", "\n]"))
        assert(arraysContainSameIds(kNeighbors(0).map(_.id), Array("2", "3", "4")))

        // instance 2
        assert(arraysContainSameIds(kNeighbors(1).map(_.id), Array("1", "3", "4")))

        // instance 3
        assert(
            arraysContainSameIds(kNeighbors(2).map(_.id), Array("2", "4", "1")) ||
            arraysContainSameIds(kNeighbors(2).map(_.id), Array("2", "4", "5"))
        )

        // instance 4
        assert(arraysContainSameIds(kNeighbors(3).map(_.id), Array("2", "3", "5")))

        // instance 5
        assert(arraysContainSameIds(kNeighbors(4).map(_.id), Array("2", "3", "4")))
    }

    test("(Iris) General original knn"){
        val LAESAConfig = new LAESA(5)
        val k = 10

        // Read rows from csv file and convert them to Instance objects
        val rawData = ReaderWriter.readCSV("datasets/iris.csv", hasHeader=false)
        val baseInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            new Instance(index.toString, attributes, classification="")
        })

        // Getting kNeighbors from ExhaustiveSearch small data
        val (exhaustiveKNeighbors, _) = ExhaustiveNeighbors.findAllNeighbors(baseInstances, k, DistanceFunctions.euclidean)
        val laesaKNeighbors = LAESAConfig.findAllKNeighbors(baseInstances, k)

        val mixedExhaustiveAndLAESAKNeighbors = exhaustiveKNeighbors.zip(laesaKNeighbors)

        var x = 0
        assert(mixedExhaustiveAndLAESAKNeighbors.forall(tuple => {
            val (exhaustive, laesa) = tuple
            val result = arraysContainSameNeighbors(exhaustive, laesa)
            if(!result){
                println(s"Ocurre en ${x}")
                println(exhaustive.map(n => s"${n.id}: ${n.distance}").mkString("[\n\t", ",\n\t", "]"))
                println(laesa.map(n => s"${n.id}: ${n.distance}").mkString("[\n\t", ",\n\t", "]"))
            }
            x += 1
            result
        }))
    }

    test("(Iris) General custom knn"){
        val LAESAConfig = new LAESA(10)
        val k = 10

        // Read rows from csv file and convert them to Instance objects
        val rawData = ReaderWriter.readCSV("datasets/iris.csv", hasHeader=false)
        val baseInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            new Instance(index.toString, attributes, classification="")
        })

        // Getting kNeighbors from ExhaustiveSearch small data
        val (exhaustiveKNeighbors, _) = ExhaustiveNeighbors.findAllNeighbors(baseInstances, k, DistanceFunctions.euclidean)
        val laesaKNeighbors = LAESAConfig.findAllKNeighbors(baseInstances, k, custom = true)

        val mixedExhaustiveAndLAESAKNeighbors = exhaustiveKNeighbors.zip(laesaKNeighbors)

        assert(mixedExhaustiveAndLAESAKNeighbors.forall(tuple => {
            val (exhaustive, laesa) = tuple
            arraysContainSameNeighbors(exhaustive, laesa)
        }))
    }

    test("(Iris) LAESA original vs custom"){
        val LAESAConfig = new LAESA(20)
        val k = 10

        // Read rows from csv file and convert them to Instance objects
        val rawData = ReaderWriter.readCSV("datasets/iris.csv", hasHeader=false)
        val baseInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            new Instance(index.toString, attributes, classification="")
        })

        // Getting kNeighbors from ExhaustiveSearch small data
        val customKNeighbors = LAESAConfig.findAllKNeighbors(baseInstances, k, custom = true)
        val originalKNeighbors = LAESAConfig.findAllKNeighbors(baseInstances, k)

        val mixedCustomAndOriginalKNeighbors = customKNeighbors.zip(originalKNeighbors)

        assert(mixedCustomAndOriginalKNeighbors.forall(tuple => {
            val (custom, original) = tuple
            if(custom.contains(null))
                println("Fue custom")
            if(original.contains(null))
                println("Fue original")
            arraysContainSameNeighbors(custom, original)
        }))
    }
}
