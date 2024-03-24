package rknn_outlier_detection

import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.distance.{DistanceFunctions, DistanceObject}
import rknn_outlier_detection.search.LAESA
import rknn_outlier_detection.utils.Utils

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

       println("Hi, mom!")
       val rdd = sc.parallelize(Seq(1,2,3,4,5))
       println(s"The rdd has ${rdd.count()} elements")
//         val i1 = new Instance("1", Array(1.0, 1.0), "")
//         val i2 = new Instance("2", Array(2.0, 2.0), "")
//         val i3 = new Instance("3", Array(3.0, 3.0), "")
//         val i4 = new Instance("4", Array(4.0, 4.0), "")
//         val i5 = new Instance("5", Array(5.0, 5.0), "")

//         val instances = sc.parallelize(Seq(i1, i2, i3, i4, i5))
//         val k = 3
//         val basePivots = sc.parallelize(instances.takeSample(withReplacement=false, num=1, seed=1))
//         val basePivotsIds = basePivots.map(_.id).collect()

//         println(s"Pivots are (${basePivots.count()}):")
//         basePivots.foreach(pivot => println(pivot.id))

//         val pivotsDistances = basePivots.cartesian(instances)
// //            .filter(tuple => tuple._1.id != tuple._2.id)
//             .map(tuple => {
//                 val (pivot, instance) = tuple
//                 val distance = DistanceFunctions.euclidean(pivot.attributes, instance.attributes)
//                 val distanceObject = new DistanceObject(pivot.id, instance.id, distance)
//                 (instance , distanceObject)
//             })
//             .groupByKey()

//         pivotsDistances.foreach(pD => {
//             val (instance, distances) = pD
//             println(s"${instance.id}: ${distances.map(d => s"${d.pivotId}: ${d.distance}").mkString(",\n")}")
//         })

//         // Initialize kNeighbors with basePivots
//         val kNeighbors = pivotsDistances.map(tuple => {
//             val (instance, distances) = tuple
//             val arr = Array.fill[KNeighbor](k)(null)
//             distances.foreach(distanceObj => {
//                 if(distanceObj.pivotId != instance.id && (arr.contains(null) || distanceObj.distance < arr.last.distance)){
//                     val newKNeighbor = new KNeighbor(distanceObj.pivotId, distanceObj.distance)
//                     Utils.addNewNeighbor(arr, newKNeighbor)
//                 }
//             })
//             (instance.id, arr)
//         })

//        println("kNeighbors iniciales:")
//        kNeighbors.foreach(kn => {
//            val (instanceId, arr) = kn
//            var neighbors = ""
//            if(arr.length > 0){
//                neighbors = arr.map(n => s"${n.id} - ${n.distance}").mkString(",\n")
//            }
//            else{
//                neighbors = "no tiene"
//            }
//            println(s"${instanceId}:\n${neighbors}")
//        })

    //     val queryWithInstanceCotas = pivotsDistances.cartesian(pivotsDistances)
    //         .filter(tuple => tuple._1._1.id != tuple._2._1.id)
    //         .map(tuple => {
    //             val (queryTuple, instanceTuple) = tuple
    //             val (query, queryDistances) = queryTuple
    //             val (instance, instanceDistances) = instanceTuple
    //             val reversedQueryDistances = queryDistances.map(distanceObj => (distanceObj.pivotId, distanceObj.distance))
    //             val reversedInstanceDistances = instanceDistances.map(distanceObj => (distanceObj.pivotId, distanceObj.distance))
    //             val allDistances = Array(reversedQueryDistances, reversedInstanceDistances).flatten
    //             val groupedByKey = allDistances.groupBy(tuple => tuple._1)
    //             val cota = groupedByKey.map(tuple => {
    //                 val (pivotId, distances) = tuple
    //                 distances.map(_._2).reduce((x, y) => math.abs(x - y))
    //             }).max

    //             (query.id, (instance, cota))
    //         })
    //         .groupByKey()

    //     val instancesById = instances.map(instance => (instance.id, instance))

    //     val allMixed = instancesById.join(queryWithInstanceCotas).join(kNeighbors).map(mixedValues => {
    //         val (instanceId, values) = mixedValues
    //         val (tuple, kNeighbors) = values
    //         val (query, instanceWithCotas) = tuple

    //         (query, kNeighbors, instanceWithCotas)
    //     })

    //     val result = allMixed.map(tuple => {
    //         val (query, kNeighbors, instancesWithCotas) = tuple
    //         instancesWithCotas.filter(instance => !basePivotsIds.contains(instance._1.id)).foreach(pair => {
    //             val (instance, cota) = pair
    //             if(kNeighbors.contains(null)){
    //                 Utils.addNewNeighbor(kNeighbors, new KNeighbor(instance.id, DistanceFunctions.euclidean(query.attributes, instance.attributes)))
    //             }
    //             else{
    //                 if(cota <= kNeighbors.last.distance){
    //                     val distance = DistanceFunctions.euclidean(query.attributes, instance.attributes)
    //                     if(distance < kNeighbors.last.distance){
    //                         Utils.addNewNeighbor(kNeighbors, new KNeighbor(instance.id, distance))
    //                     }
    //                 }
    //             }
    //         })

    //         (query.id, kNeighbors)
    //     }).collect().sortWith((a, b) => a._1 < b._1)

    //     println(s"kNeighbors tiene length de: ${result.length} y son ${result.map(r => r._1).mkString("Array(", ", ", ")")}")
    //     println(result.map(t => s"${t._1}:\n\t${t._2.map(n => s"${n.id}: ${n.distance}").mkString(",\n\t")}").mkString("\n\n"))

    //     println("Todos los batches tienen el mismo length que k: ", result.forall(pair => pair._2.length == k))
    //     println(s"Instancia 1 esta bien: ${arraysContainSameIds(result(0)._2.map(_.id), Array("2", "3", "4"))}")
    //     println(s"Instancia 2 esta bien: ${arraysContainSameIds(result(1)._2.map(_.id), Array("1", "3", "4"))}")
    //     println(s"Instancia 3 esta bien: ${arraysContainSameIds(result(2)._2.map(_.id), Array("2", "4", "1")) ||
    //         arraysContainSameIds(result(2)._2.map(_.id), Array("2", "4", "5"))}")
    //     println(s"Instancia 4 esta bien: ${arraysContainSameIds(result(3)._2.map(_.id), Array("2", "3", "5"))}")
    //     println(s"Instancia 5 esta bien: ${arraysContainSameIds(result(4)._2.map(_.id), Array("2", "3", "4"))}")

    }

    // def arraysContainSameNeighbors(arr1: Array[KNeighbor], arr2: Array[KNeighbor]): Boolean = {
    //     if(arr1.length != arr2.length) return false
    //     if(arr1.isEmpty) return false

    //     var sameElements = true
    //     val arr1Ids = arr1.map(_.id)
    //     val arr2Ids = arr2.map(_.id)
    //     var index = 0

    //     while(sameElements && index < arr1Ids.length){
    //         if(!arr2Ids.contains(arr1Ids(index))) sameElements = false
    //         index += 1
    //     }

    //     if(sameElements) return true

    //     val arr1SortedDist = arr1.map(_.distance).sorted
    //     val arr2SortedDist = arr2.map(_.distance).sorted

    //     arr1SortedDist.zip(arr2SortedDist).forall(pair => pair._1 == pair._2)
    // }

    // def arraysContainSameIds(arr1: Array[String], arr2: Array[String]): Boolean = {
    //     if(arr1.length != arr2.length || arr1.isEmpty) return false

    //     val sortedArr1 = arr1.sorted
    //     val sortedArr2 = arr2.sorted

    //     sortedArr1.sameElements(sortedArr2)
    // }
}
