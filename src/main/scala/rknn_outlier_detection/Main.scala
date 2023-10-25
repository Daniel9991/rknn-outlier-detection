package rknn_outlier_detection

import custom_objects.{Instance, KNeighbor}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import utils.DistanceFunctions

import scala.collection.mutable.ArrayBuffer

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def readCSV(filePath: String): RDD[Array[String]] ={
        val lines = sc.textFile(filePath)
        lines.map(line => line.split(",").map(_.trim))
    }

    def countTokensInCSV(tokens: RDD[Array[String]]): Int={
        val lineSizes = tokens.map(_.length)
        val sum = lineSizes.sum()
        sum.toInt
    }

    def findKNeighborsForAll(instances: RDD[Instance], k: Int): RDD[Instance]={

        instances.foreach(instance => {
            val otherInstances = instances.filter(otherInstance => otherInstance.id != instance.id)
            val allKNeighbors = instances.map(otherInstance => new KNeighbor(otherInstance.id, DistanceFunctions.euclidean(instance.attributes, otherInstance.attributes)))
            val kNeighbors = allKNeighbors.aggregate(new ArrayBuffer[KNeighbor])(
                (acc, neighbor) => {
                    if(acc.length < k){
                        acc.addOne(neighbor)
                        acc.sortWith((n1, n2) => n1.distance < n2.distance)
                    }
                    else if(neighbor.distance < acc.last.distance){
                        acc(k - 1) = neighbor
                        acc.sortWith((n1, n2) => n1.distance < n2.distance)
                    }
                    else{
                        acc
                    }
                },
                (acc1, acc2) => {
                    acc1.addAll(acc2)
                    acc1.sortWith((n1, n2) => n1.distance < n2.distance)
                    acc1.slice(0, k)
                }
            )

            instance.kNeighbors = kNeighbors.toArray
        })

        instances
    }

    def sortNeighbors(n1: KNeighbor, n2: KNeighbor): Boolean ={
        n1.distance < n2.distance
    }

    def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    def main(args: Array[String]): Unit ={

        //        val FILE_PATH = "datasets/Iris-virginica_Iris-setosa_4.csv"
        //        val k = 21
        //        val topN = 6
        //
        //        val data = readCSV(FILE_PATH)
        //        val instances = data.zipWithIndex.map(tuple => {
        //            val (line, index) = tuple
        //            new custom_objects.Instance(index.toString, line.slice(0, 4).map(_.toDouble), line.last)
        //        })
        //
        //        val fullyMappedInstances = instances.cartesian(instances)
        //            .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
        //            .map(instances_tuple => {
        //                val (ins1, ins2) = instances_tuple
        //                (
        //                    ins1.id,
        //                    new custom_objects.KNeighbor(
        //                        ins2.id,
        //                        DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)
        //                    )
        //                )
        //            })
        //
        //        val sortNeighbors = (n1: custom_objects.KNeighbor, n2: custom_objects.KNeighbor) => n1.distance < n2.distance
        //
        //        val groupedCombinations = fullyMappedInstances.groupByKey()
        //        val instancesWithNeighbors = groupedCombinations.map(tuple => {
        //            val (instanceId, neighbors) = tuple
        //            (
        //                instanceId,
        //                neighbors.toArray.sortWith(sortNeighbors).slice(0, k))
        //        })
        //
        //        val neighborReferences = instancesWithNeighbors.flatMap(tuple => {
        //            val (instanceId, neighbors) = tuple
        //            neighbors.map(neighbor => (neighbor.id, instanceId))
        //        })
        //
        //        val y = neighborReferences.groupByKey()
        //            .mapValues(rNeighbors => rNeighbors.map(
        //                    rNeighbor => new custom_objects.Neighbor(rNeighbor)
        //            ).toArray)
        //
        //        val antihubValues = y.mapValues(reverseNeighbors => normalizeReverseNeighborsCount(reverseNeighbors.length))
        //
        //        val sortedAntihubValues = antihubValues.sortBy(_._2, ascending = false)
        //        val anomalousIds = sortedAntihubValues.take(topN).map(tuple => tuple._1)
        //        anomalousIds.foreach(id => println(id))
        //
        //        val results = antihubValues.map(tuple => (tuple._1, if(anomalousIds.contains(tuple._1)) "Iris-setosa" else "Iris-virginica"))
        //
        //        val realClassifications = instances.map(instance => (instance.id, instance.classification))
        //        val realAndResults = results.join(realClassifications).sortBy(tuple => tuple._1.toInt)
        //
        //        realAndResults.foreach(tuple => {
        //            val (id, classifications) = tuple
        //            val (result, real) = classifications
        //
        //            println(s"instance ${id} is ${real} and was ${result}: ${if (real == result) "match" else "no match" }")
        //        })

        println("hi, mom!")
    }
}
