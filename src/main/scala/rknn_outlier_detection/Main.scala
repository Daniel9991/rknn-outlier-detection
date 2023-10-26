package rknn_outlier_detection

import custom_objects.{Instance, KNeighbor, Neighbor}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.detection.Techniques
import rknn_outlier_detection.search.ExhaustiveSearch
import rknn_outlier_detection.utils.Utils.{readCSV, sortNeighbors}
import utils.{DistanceFunctions, Utils}

import scala.collection.mutable.ArrayBuffer

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

        val FILE_PATH = "datasets/Iris-virginica_Iris-setosa_4.csv"
        val k = 21
        val topN = 6

//        Read data and convert to instances

        val data = readCSV(FILE_PATH, sc)

        val instances = data.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            new custom_objects.Instance(index.toString, line.slice(0, 4).map(_.toDouble), line.last)
        })

//        Find kNeighbors

        val x = ExhaustiveSearch.getKNeighbors(instances, k)
        val x2 = ExhaustiveSearch.findKNeighborsForAll(instances, k)

        println(s"son iguales ambos knn search: ${x.join(x2).filter(tuple => {
            val (key, nestedTuple) = tuple
            val (ns1, ns2) = nestedTuple

            ns1.length == ns2.length && Utils.arrayEquals(ns1.map(_.id), ns2.map(_.id), ns1.length)
        }).count() == x.join(x2).count()}")

//        Find reverse neighbors

//        val y = ExhaustiveSearch.getReverseNeighbors(x)
//
////        Add neighbors and reverse neighbors to instances
//
//        val equippedInstances = instances.map(instance => (instance.id, instance))
//            .join(x)
//            .map(tuple => {
//                val (key, nestedTuple) = tuple
//                val (instance, kNeighbors) = nestedTuple
//                instance.kNeighbors = kNeighbors
//                (key, instance)
//            })
//            .join(y)
//            .map(tuple => {
//                val (key, nestedTuple) = tuple
//                val (instance, rNeighbors) = nestedTuple
//                instance.rNeighbors = rNeighbors
//                instance
//            })
//
//        val joinedKAndRNeighbors = x.join(y)
//
//        val antihubValues2 = Techniques.antihub(y)

//        val sortedAntihubValues = antihubValues.sortBy(_._2, ascending = false)
//        val anomalousIds = sortedAntihubValues.take(topN).map(tuple => tuple._1)
//        anomalousIds.foreach(id => println(id))
//
//        val results = antihubValues.map(tuple => (tuple._1, if(anomalousIds.contains(tuple._1)) "Iris-setosa" else "Iris-virginica"))
//
//        val realClassifications = instances.map(instance => (instance.id, instance.classification))
//        val realAndResults = results.join(realClassifications).sortBy(tuple => tuple._1.toInt)
//
//        println(realAndResults.filter(tuple => {
//            val (id, classifications) = tuple
//            val (result, real) = classifications
//
//            println(s"instance ${id} is ${real} and was ${result}: ${if (real == result) "match" else "no match" }")
//            real == result
//        }).count() == realAndResults.count())

    }
}
