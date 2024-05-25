package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.shared.utils.Utils.addNewNeighbor
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

import scala.collection.mutable

/*
* This class implements a knn pivot-based search method for distributed environments based
* on the following paper. https://link.springer.com/chapter/10.1007/978-3-319-71246-8_51
*/

class PkNN[A](
    pivotsAmount: Int,
) extends KNNSearchStrategy[A] with Serializable{

//    val pivotSelector = new IncrementalSelection[A](700, 800)

    // Multistep pknn
    // 1. knn for each point of each cell -> core knn
    // 2. find core distance for each cell (max distance of a point to its k-neighbor)
    // 3. find support distance for each cell (max sum of pivot-to-instance distance + instance-to-kneighbor distance)
    // 4. find supporting cells for each cell (all cells for which support distance > half of distance between cells pivots)
    // 5. prune the support sets for each cell, eliminating all support candidates whose
    // rest between the distance (|vi, q| - |vj, q|)/2 >= core-distance(vi)
    // 6. remove outliers to balance nodes and support sets

    def getKeyFromInstancesIds(id1: String, id2: String): String = {
        if(id1 < id2) s"${id1}_${id2}" else s"${id2}_${id1}"
    }

    def getDistancesMap(pivots: Array[Instance[A]], distanceFunction: DistanceFunction[A]): mutable.Map[String, Double] ={
        val pivotDistances = mutable.Map[String, Double]()
        for (i <- pivots.indices){
            val i1 = pivots(i)
            for(j <- i + 1 until pivots.length){
                val i2 = pivots(j)
                pivotDistances(getKeyFromInstancesIds(i1.id, i2.id)) = distanceFunction(i1.data, i2.data)
            }
        }
        pivotDistances
    }

    override def findKNeighbors(instances: RDD[Instance[A]], k: Int, distanceFunction: DistanceFunction[A], sc: SparkContext): RDD[(String, Array[KNeighbor])] = {

        val pivots = findBasePivots(instances, sc)
//        val pivots = sc.parallelize(pivotSelector.findPivots(instances, pivotsAmount,distanceFunction))

        // Finding pivots distances and broadcasting them
        val localPivots = pivots.collect()
        val pivotDistances = getDistancesMap(localPivots, distanceFunction)
        val broadcastedPivotDistances = sc.broadcast(pivotDistances)


        // Create cells
        val cells = instances.cartesian(pivots)
            .map(tuple => {
                val (instance, pivot) = tuple
                (instance, (pivot, distanceFunction(instance.data, pivot.data)))
            })
            .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
            .map(t => (t._2._1, (t._1, t._2._2)))


        cells.cache()

//        val cellsLengths = cells.map(cell => cell._2.toArray.length).collect()
//        println(s"Total cells: ${cellsLengths.length}")
//        println(s"Min instances in cell: ${cellsLengths.min}")
//        println(s"Max instances in cell: ${cellsLengths.max}")
//        println(s"Avg instances in cell: ${cellsLengths.sum.toDouble / cellsLengths.length.toDouble}")
//        sc.stop()

        // Step 1
        val coreKNNs = cells.groupByKey.mapValues(iterable => {
            // Assuming that an array of instances fits into memory??? Maybe used distributed version???
            val knns = new ExhaustiveSmallData().findKNeighbors(iterable.map(_._1).toArray, k, distanceFunction)
            iterable.zip(knns).map(tuple => {
                val (secondTuple, kNeighbors) = tuple
                val (instance, distanceToPivot) = secondTuple
                (instance, kNeighbors, distanceToPivot)
            })
        })

        coreKNNs.cache()

        // Step 2
        val coreDistances = coreKNNs.mapValues(knns => {
            knns.map(tuple => {
                // knn might not be full if there are less instances for a given cell than k
                if(tuple._2.last != null) tuple._2.last.distance else Double.PositiveInfinity
            }).max
        })

//        coreDistances.cache()

        // Step 3
        val supportingDistances = coreKNNs.mapValues(iterable => {
            iterable.map(tuple => {
                // knn might be empty if there are less instances for a given cell than k // TODO should it be this way???
                if(tuple._2.last != null) tuple._2.last.distance + tuple._3 else Double.PositiveInfinity
            }).max
        })



        // Step 4 Find support sets

        // Get a tuple for pivot, coreDistance, supportDistance
        val pivotsAndDistances = coreDistances.join(supportingDistances).map(tuple => (tuple._1, tuple._2._1, tuple._2._2))
        // Get combination of pivots getting a pivot and an iterable of the pivots that it supports
        val pivotsCombinations = pivotsAndDistances.cartesian(pivotsAndDistances).filter(tuple => {
            tuple._1._1.id != tuple._2._1.id &&
//                distanceFunction(tuple._1._1.data, tuple._2._1.data) / 2 <= tuple._1._3
            broadcastedPivotDistances.value(getKeyFromInstancesIds(tuple._1._1.id, tuple._2._1.id)) / 2 <= tuple._1._3
        }).map(tuple => (tuple._1._1, tuple._2)).groupByKey

        // Step 5 Prune candidate instances

        val finalSupportSets = cells.join(pivotsCombinations).flatMap(tuple => {
            val (candidateInstance, pivotsToSupport) = tuple._2
            pivotsToSupport.filter(pivotData => {
                val pruneMeasurement = math.abs(distanceFunction(pivotData._1.data, candidateInstance._1.data) - candidateInstance._2) / 2
                pruneMeasurement < pivotData._2
            }).map(pivotData => (pivotData._1, candidateInstance._1))
        }).groupByKey()


        // Correct knn search

        val finalKNNs = coreKNNs.join(finalSupportSets).flatMap(tuple => {
            val (instancesAndKNNs, supportSet) = tuple._2
            val finalKNNs = instancesAndKNNs.map(instanceAndKNeighbors => {
                val (instance, kNeighbors, _) = instanceAndKNeighbors
                supportSet.foreach(supportInstance => {
                    val distance = distanceFunction(instance.data, supportInstance.data)
                    if(kNeighbors.contains(null) || kNeighbors.last.distance > distance){
                        addNewNeighbor(kNeighbors, new KNeighbor(supportInstance.id, distance))
                    }
                })
                (instance.id, kNeighbors)
            })
            finalKNNs
        })

        finalKNNs
    }

    def findBasePivots(instances: RDD[Instance[A]], sc: SparkContext): RDD[Instance[A]] = {
        if(instances.count() == 0)
            return sc.parallelize(Seq())

        sc.parallelize(instances.takeSample(withReplacement=false, num=pivotsAmount, seed=1))
    }
}
