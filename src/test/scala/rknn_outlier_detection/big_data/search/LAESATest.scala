//package rknn_outlier_detection.big_data.search
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.scalatest.funsuite.AnyFunSuite
//import rknn_outlier_detection.big_data.search.pivot_based.LAESA
//import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
//import rknn_outlier_detection.shared.distance.DistanceFunctions
//import rknn_outlier_detection.shared.utils.ReaderWriter
//import rknn_outlier_detection.small_data.search.ExhaustiveSmallData
//
//class LAESATest extends AnyFunSuite {
//
//    def arraysContainSameNeighbors(arr1: Array[KNeighbor], arr2: Array[KNeighbor]): Boolean = {
//        if(arr1.length != arr2.length) return false
//        if(arr1.isEmpty) return false
//
//        var sameElements = true
//        val arr1Ids = arr1.map(_.id)
//        val arr2Ids = arr2.map(_.id)
//        var index = 0
//
//        while(sameElements && index < arr1Ids.length){
//            if(!arr2Ids.contains(arr1Ids(index))) sameElements = false
//            index += 1
//        }
//
//        if(sameElements) return true
//
//        val arr1SortedDist = arr1.map(_.distance).sorted
//        val arr2SortedDist = arr2.map(_.distance).sorted
//
//        arr1SortedDist.zip(arr2SortedDist).forall(pair => pair._1 == pair._2)
//    }
//
//    def arraysContainSameIds(arr1: Array[String], arr2: Array[String]): Boolean = {
//        if(arr1.length != arr2.length || arr1.isEmpty) return false
//
//        val sortedArr1 = arr1.sorted
//        val sortedArr2 = arr2.sorted
//
//        sortedArr1.sameElements(sortedArr2)
//    }
//
//    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))
//    val searchStrategy = new LAESA(3)
//
//    val i1 = new Instance("1", Array(1.0, 1.0), "")
//    val i2 = new Instance("2", Array(2.0, 2.0), "")
//    val i3 = new Instance("3", Array(3.0, 3.0), "")
//    val i4 = new Instance("4", Array(4.0, 4.0), "")
//    val i5 = new Instance("5", Array(5.0, 5.0), "")
//    val i6 = new Instance("6", Array(1.9, 1.9), "")
//    val i7 = new Instance("7", Array(2.2, 2.2), "")
//
//    test("Empty RDD"){
//        val testingData = sc.parallelize(Seq[Instance]())
//        val result = searchStrategy.findKNeighbors(testingData, 1, sc)
//        assert(result.isEmpty())
//    }
//
//    test("k less than 1"){
//        // TODO: Implement this test
//        // Create UnacceptableKValue and throw it in findKNeighbors implementations
//        // if k <= 0 or k >= n
//    }
//
//    test("General knn and rknn"){
//        val k = 3
//        val testingData = sc.parallelize(Seq(i1, i2, i3, i4, i5))
//
//        val kNeighborsRDD = searchStrategy.findKNeighbors(testingData, k, sc)
//        val kNeighbors = kNeighborsRDD.collect()
//
//        assert(kNeighbors.forall(pair => pair._2.length == k))
//
//        println(kNeighbors.map(t => s"${t._1}:\n\t${t._2.map(n => s"${n.id}: ${n.distance}").mkString(",\n\t")}").mkString("\n\n"))
//        val sortedKNeighbors = kNeighbors.sortWith((a, b) => a._1 < b._1)
//
//        // instance1
//        assert(arraysContainSameIds(sortedKNeighbors(0)._2.map(_.id), Array("2", "3", "4")))
//
//        // instance2
//        assert(arraysContainSameIds(sortedKNeighbors(1)._2.map(_.id), Array("1", "3", "4")))
//
//        // instance3
//        assert(
//            arraysContainSameIds(sortedKNeighbors(2)._2.map(_.id), Array("2", "4", "1")) ||
//                arraysContainSameIds(sortedKNeighbors(2)._2.map(_.id), Array("2", "4", "5"))
//        )
//
//        // instance4
//        assert(arraysContainSameIds(sortedKNeighbors(3)._2.map(_.id), Array("2", "3", "5")))
//
//        // instance5
//        assert(arraysContainSameIds(sortedKNeighbors(4)._2.map(_.id), Array("2", "3", "4")))
//    }
//
//    test("(Iris) General knn and rknn"){
//        val k = 10
//
//        // Read rows from csv file and convert them to Instance objects
//        val rawData = ReaderWriter.readCSV("datasets/iris.csv", hasHeader=false)
//        val baseInstances = rawData.zipWithIndex.map(tuple => {
//            val (line, index) = tuple
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            new Instance(index.toString, attributes, classification="")
//        })
//
//        // Getting kNeighbors from ExhaustiveSearch small data
//        val (smallKNeighbors, _) = ExhaustiveSmallData.findAllNeighbors(baseInstances, k, DistanceFunctions.euclidean)
//
//        // Getting kNeighbors from ExhaustiveSearch big data
//        val rdd = sc.parallelize(baseInstances.toSeq)
//        val kNeighborsRDD = searchStrategy.findKNeighbors(rdd, k, sc)
//        val bigKNeighbors = kNeighborsRDD
//            .collect()
//            .map(tuple => (tuple._1.toInt, tuple._2))
//            .sortWith((t1, t2) => t1._1 < t2._1)
//            .map(_._2)
//
//        val mixedSmallAndBigKNeighbors = smallKNeighbors.zip(bigKNeighbors)
//
//        assert(smallKNeighbors.length == bigKNeighbors.length)
//        assert(mixedSmallAndBigKNeighbors.forall(tuple => {
//            val (small, big) = tuple
//            arraysContainSameNeighbors(small, big)
//        }))
//    }
//}
