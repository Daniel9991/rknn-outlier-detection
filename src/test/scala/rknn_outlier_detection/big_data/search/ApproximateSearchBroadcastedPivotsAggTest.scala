package rknn_outlier_detection.big_data.search

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.big_data.detection.Antihub
import rknn_outlier_detection.big_data.search.pivot_based.GroupedByPivot
import rknn_outlier_detection.big_data.search.reverse_knn.NeighborsReverser
import rknn_outlier_detection.euclidean
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}

import scala.annotation.tailrec

/*
    In this class a new tail recursive implementation for kNeighbors aggregation
    between partitions is tested against classic imperative way
* */
class ApproximateSearchBroadcastedPivotsAggTest extends AnyFunSuite{

    test("search scale test"){
        val sc = new SparkContext(
            new SparkConf()
                .setMaster("local[*]")
                .setAppName("Test Approximate Search with Broadcasted Pivots")
                .set("spark.executor.memory", "12g")
                .set("spark.default.parallelism", "48")
        )

        val pivotsAmount = 25
        val k = 1000
        val seed = 56342
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))

            val pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed=seed)

            val onStartIter = System.nanoTime
            val kNeighborsIter = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc).cache()
            kNeighborsIter.count()
            val onFinishIter = System.nanoTime
            val iterDuration = (onFinishIter - onStartIter) / 1000000

            val onStartTailRec = System.nanoTime
            val kNeighborsTailRec = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc, tailRec = true).cache()
            kNeighborsTailRec.count()
            val onFinishTailRec = System.nanoTime
            val tailRecDuration = (onFinishTailRec - onStartTailRec) / 1000000

            val rNeighborsTailRec = NeighborsReverser.findReverseNeighbors(kNeighborsTailRec)
            val antihubTailRec = new Antihub().antihub(rNeighborsTailRec)
            val predictionsAndLabelsAntihubTailRec = classifications.join(antihubTailRec).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihubTailRec = new BinaryClassificationMetrics(predictionsAndLabelsAntihubTailRec)

            val rNeighborsIter = NeighborsReverser.findReverseNeighbors(kNeighborsIter)
            val antihubIter = new Antihub().antihub(rNeighborsIter)
            val predictionsAndLabelsAntihubIter = classifications.join(antihubIter).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihubIter = new BinaryClassificationMetrics(predictionsAndLabelsAntihubIter)

            println(s"k: $k, pivots: $pivotsAmount, seed: $seed, tailRecDuration: ${tailRecDuration}ms, tailRecAntihub: ${detectionMetricsAntihubTailRec.areaUnderROC()}, iterDuration: ${iterDuration}ms, iterAntihub: ${detectionMetricsAntihubIter.areaUnderROC()}")
            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def inPartitionAggregator(acc: Array[KNeighbor], newNeighbor: KNeighbor): Array[KNeighbor] = {
        @tailrec
        def inPartitionAggregatorTailRec(added: Array[KNeighbor], remaining: Array[KNeighbor], newNeighbor: KNeighbor): Array[KNeighbor] = {
            if(!added.contains(null) || (remaining.length == 0 && newNeighbor == null)){
                added
            }
            else{
                val nextNullPosition = added.indexOf(null)
                val copy = added.map(identity)
                if(remaining.length == 0 || (newNeighbor != null && remaining(0).distance > newNeighbor.distance)){
                    copy(nextNullPosition) = newNeighbor
                    inPartitionAggregatorTailRec(copy, remaining, null)
                }
                else{
                    copy(nextNullPosition) = remaining(0)
                    val newRemaining = remaining.slice(1, remaining.length)
                    inPartitionAggregatorTailRec(copy, newRemaining, newNeighbor)
                }
            }
        }

        if(acc.last != null && acc.last.distance <= newNeighbor.distance){
            acc
        }
        else{
            val k = acc.length
            inPartitionAggregatorTailRec(Array.fill[KNeighbor](k)(null), acc.filter(n => n != null), newNeighbor)
        }
    }

    val kNeighbor1 = new KNeighbor(1, 0.34)
    val kNeighbor2 = new KNeighbor(2, 0.38)
    val kNeighbor3 = new KNeighbor(3, 0.56)
    val kNeighbor4 = new KNeighbor(4, 0.689)
    val kNeighbor5 = new KNeighbor(5, 0.945)
    val kNeighbor6 = new KNeighbor(6, 2.54)
    val kNeighbor7 = new KNeighbor(7, 3.689)
    val kNeighbor8 = new KNeighbor(8, 0.689)
    val kNeighbor9 = new KNeighbor(9, 0.945)

    test("inPartitionAggregator empty current neighbors"){
        val current: Array[KNeighbor] = Array(null, null, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor1)
        assert(result.length === 7)
        assert(result(0) !== null)
        assert(result(0).id == 1)
        assert(result.slice(1, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator 1 current neighbor (insert after)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, null, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor2)
        assert(result.length === 7)
        assert(result(0) !== null)
        assert(result(0).id == 1)
        assert(result(1) !== null)
        assert(result(1).id == 2)
        assert(result.slice(2, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator 1 current neighbor (insert before)"){
        val current: Array[KNeighbor] = Array(kNeighbor2, null, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor1)
        assert(result.length === 7)
        assert(result(0) !== null)
        assert(result(0).id == 1)
        assert(result(1) !== null)
        assert(result(1).id == 2)
        assert(result.slice(2, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator 2 current neighbor (insert third)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor3)
        assert(result.length === 7)
        assert(result(0) !== null)
        assert(result(0).id == 1)
        assert(result(1) !== null)
        assert(result(1).id == 2)
        assert(result(2) !== null)
        assert(result(2).id == 3)
        assert(result.slice(3, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator 2 current neighbor (insert second)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor3, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor2)
        assert(result.length === 7)
        assert(result(0) !== null)
        assert(result(0).id == 1)
        assert(result(1) !== null)
        assert(result(1).id == 2)
        assert(result(2) !== null)
        assert(result(2).id == 3)
        assert(result.slice(3, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator 2 current neighbor (insert first)"){
        val current: Array[KNeighbor] = Array(kNeighbor2, kNeighbor3, null, null, null, null, null)
        val result = inPartitionAggregator(current, kNeighbor1)
        assert(result.length === 7)
        assert((result(0) !== null) && (result(0).id == 1))
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert(result.slice(3, result.length).forall(e => e == null))
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (insert last)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor6)
        val result = inPartitionAggregator(current, kNeighbor5)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 5)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (insert middle)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor4, kNeighbor5, kNeighbor6)
        val result = inPartitionAggregator(current, kNeighbor3)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 5)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }



    test("inPartitionAggregator full current neighbors (insert first)"){
        val current: Array[KNeighbor] = Array(kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor5, kNeighbor6)
        val result = inPartitionAggregator(current, kNeighbor1)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 5)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (no insert)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor5)
        val result = inPartitionAggregator(current, kNeighbor6)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 5)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (same insert first)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor5)
        val result = inPartitionAggregator(current, kNeighbor1)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 1)
        assert((result(2) !== null) && result(2).id == 2)
        assert((result(3) !== null) && result(3).id == 3)
        assert((result(4) !== null) && result(4).id == 4)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (same insert second to last)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor5)
        val result = inPartitionAggregator(current, kNeighbor8)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 8)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    test("inPartitionAggregator full current neighbors (same last dist but no insert)"){
        val current: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3, kNeighbor4, kNeighbor5)
        val result = inPartitionAggregator(current, kNeighbor9)
        assert(result.length === 5)
        assert((result(0) !== null) && result(0).id == 1)
        assert((result(1) !== null) && result(1).id == 2)
        assert((result(2) !== null) && result(2).id == 3)
        assert((result(3) !== null) && result(3).id == 4)
        assert((result(4) !== null) && result(4).id == 5)
//        println(result.map(r => if(r !== null) r.id else "null").mkString("[ ", ", ", " ]"))
    }

    def betweenPartitionsAggregator(acc1: Array[KNeighbor], acc2: Array[KNeighbor]): Array[KNeighbor] = {
        @tailrec
        def betweenPartitionsAggregatorTailRec(merged: Array[KNeighbor], remaining1: Array[KNeighbor], remaining2: Array[KNeighbor]): Array[KNeighbor] = {
            if(!merged.contains(null) || remaining1.length == 0 && remaining2.length == 0){
                merged
            }
            else{
                val nextNullPosition = merged.indexOf(null)
                val copy = merged.map(identity)
                if(remaining2.length == 0 || (remaining1.length > 0 && remaining1(0).distance <= remaining2(0).distance )){
                    copy(nextNullPosition) = remaining1(0)
                    val newRemaining1 = remaining1.slice(1, remaining1.length)
                    betweenPartitionsAggregatorTailRec(copy, newRemaining1, remaining2)
                }
                else{
                    copy(nextNullPosition) = remaining2(0)
                    val newRemaining2 = remaining2.slice(1, remaining2.length)
                    betweenPartitionsAggregatorTailRec(copy, remaining1, newRemaining2)
                }
            }
        }

        val k = acc1.length
        val resulting = Array.fill[KNeighbor](k)(null)
        betweenPartitionsAggregatorTailRec(resulting, acc1.filter(n => n != null), acc2.filter(n => n != null))
    }

//    val kNeighbor1 = new KNeighbor(1, 0.34)
//    val kNeighbor2 = new KNeighbor(2, 0.38)
//    val kNeighbor3 = new KNeighbor(3, 0.56)
//    val kNeighbor4 = new KNeighbor(4, 0.689)
//    val kNeighbor5 = new KNeighbor(5, 0.945)
//    val kNeighbor6 = new KNeighbor(6, 2.54)
//    val kNeighbor7 = new KNeighbor(7, 3.689)
//    val kNeighbor8 = new KNeighbor(8, 0.689)
//    val kNeighbor9 = new KNeighbor(9, 0.945)

    test("betweenPartitionsAgg agg1 1 element agg2 empty"){
        val agg1: Array[KNeighbor] = Array(kNeighbor1, null, null)
        val agg2: Array[KNeighbor] = Array(null, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert(result(1) == null && result(2) == null)
    }

    test("betweenPartitionsAgg agg1 empty agg2 1 element"){
        val agg1: Array[KNeighbor] = Array(null, null, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert(result(1) == null && result(2) == null)
    }

    test("betweenPartitionsAgg 1 element both aggs"){
        val agg1: Array[KNeighbor] = Array(kNeighbor2, null, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert(result(2) == null)
    }

    test("betweenPartitionsAgg agg1 1 element agg2 2 elements"){
        val agg1: Array[KNeighbor] = Array(kNeighbor2, null, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, kNeighbor3, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 2 elements agg2 1 element"){
        val agg1: Array[KNeighbor] = Array(kNeighbor2, kNeighbor3, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 full agg2 empty"){
        val agg1: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3)
        val agg2: Array[KNeighbor] = Array(null, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 empty agg2 full"){
        val agg1: Array[KNeighbor] = Array(null, null, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, kNeighbor2, kNeighbor3)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 full agg2 1 element"){
        val agg1: Array[KNeighbor] = Array(kNeighbor1, kNeighbor3, kNeighbor4)
        val agg2: Array[KNeighbor] = Array(kNeighbor2, null, null)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 1 element agg2 full"){
        val agg1: Array[KNeighbor] = Array(kNeighbor2, null, null)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, kNeighbor3, kNeighbor4)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }

    test("betweenPartitionsAgg agg1 full element agg2 full"){
        val agg1: Array[KNeighbor] = Array(kNeighbor2, kNeighbor5, kNeighbor6)
        val agg2: Array[KNeighbor] = Array(kNeighbor1, kNeighbor3, kNeighbor4)
        val result = betweenPartitionsAggregator(agg1, agg2)
        assert(result.length == 3)
        assert((result(0) != null) && result(0).id == 1)
        assert((result(1) != null) && result(1).id == 2)
        assert((result(2) != null) && result(2).id == 3)
    }
}
