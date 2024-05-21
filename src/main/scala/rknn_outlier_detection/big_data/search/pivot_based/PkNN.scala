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

/*
* This class implements a knn pivot-based search method for distributed environments based
* on the following paper. https://link.springer.com/chapter/10.1007/978-3-319-71246-8_51
*/

class PkNN(
    pivotsAmount: Int,
) extends KNNSearchStrategy {

    // initial n pivots are chosen
    // how to choose the pivots???

    // group instances based on their distances to the pivots
    // creating Voronoi cells

    // each partition needs a support set for the cell so that all neighbors can be found locally

    // Multistep pknn
    // 1. knn for each point of each cell -> core knn
    // 2. find core distance for each cell (max distance of a point to its k-neighbor)
    // 3. find support distance for each cell (max sum of pivot-to-instance distance + instance-to-kneighbor distance)
    // 4. find supporting cells for each cell (all cells for which support distance > half of distance between cells pivots)
    // 5. prune the support sets for each cell, eliminating all support candidates whose
    // rest between the distance (|vi, q| - |vj, q|)/2 >= core-distance(vi)
    // 6. remove outliers to balance nodes and support sets

    override def findKNeighbors(instances: RDD[Instance], k: Int, distanceFunction: DistanceFunction, sc: SparkContext): RDD[(String, Array[KNeighbor])] = {

        val pivots = findBasePivots(instances, sc)

        // Create cells
        val cells = instances.cartesian(pivots)
            .map(tuple => {
                val (instance, pivot) = tuple
                (instance, (pivot, distanceFunction(instance.attributes, pivot.attributes)))
            })
            .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
            .map(t => (t._2._1, (t._1, t._2._2)))
            .groupByKey

        cells.cache()

        // Step 1
        val coreKNNs = cells.mapValues(iterable => {
            // Assuming that an array of instances fits into memory??? Maybe used distributed version???
            val knns = ExhaustiveSmallData.findKNeighbors(iterable.map(_._1).toArray, k, distanceFunction)
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

        coreDistances.cache()

        // Step 3
        val supportingDistances = coreKNNs.mapValues(iterable => {
            iterable.map(tuple => {
                // knn might be empty if there are less instances for a given cell than k // TODO should it be this way???
                if(tuple._2.last != null) tuple._2.last.distance + tuple._3 else Double.PositiveInfinity
            }).max
        })

        supportingDistances.cache()

        // Step 4
//        val supportingCells = supportingDistances.cartesian(cells)
//            .filter(tuple => tuple._1._1.id != tuple._2._1.id)
//            .groupByKey()
//            .map(tuple => {
//                val (cellAndSupportDistance, allOtherCells) = tuple
//                val (cell, supportDistance) = cellAndSupportDistance
//
//                // TODO Iterable has no filter method? Need to find array alternative
//                val supportCells = allOtherCells
//                    .toArray
//                    .filter(otherCell => {
//                        val (cellPivot, _) = otherCell
//                        DistanceFunctions.euclidean(cell.attributes, cellPivot.attributes) / 2 <= supportDistance
//                    })
//                    .flatMap(tuple => tuple._2)
//
//                (cell, supportCells)
//            })

        val supportingCells = supportingDistances.cartesian(cells)
            .filter(tuple => {
                tuple._1._1.id != tuple._2._1.id &&
                    distanceFunction(tuple._1._1.attributes, tuple._2._1.attributes) / 2 <= tuple._1._2
            })
            .mapValues(otherCell => otherCell._2).map(tuple => (tuple._1._1, tuple._2.toArray))

        //        println(s"supporting cells amount:${supportingCells.collect.map(t => s"\n${t._1.id}: ${t._2.length}").mkString("")}")

        supportingCells.cache()

        // Step 5
        val pruningMeasures = supportingCells.map(cellAndSupportingInstances => {
            val (cell, supportingInstances) = cellAndSupportingInstances
            val supportingInstancesAndPruneValue = supportingInstances.map(supportInstanceAndDistanceToItsPivot => {
                val (supportInstance, distanceToItsPivot) = supportInstanceAndDistanceToItsPivot
                val pruneValue = math.abs(DistanceFunctions.euclidean(cell.attributes, supportInstance.attributes) - distanceToItsPivot) / 2
                (supportInstance, pruneValue)
            })
            (cell, supportingInstancesAndPruneValue)
        })

        pruningMeasures.cache()

        val finalSupportSets = coreDistances.join(pruningMeasures)
            .map(tuple => {
                val (instance, coreDistanceAndCandidateSupportInstances) = tuple
                val (coreDistance, candidateSupportInstances) = coreDistanceAndCandidateSupportInstances
                val supportSet = candidateSupportInstances.filter(tuple => tuple._2 < coreDistance).map(_._1)
                (instance, supportSet)
            })

        finalSupportSets.cache()

        // Step 6

        // Adjust knn with support sets
        val trimmedCoreKNNs = coreKNNs.map(tuple => {
            val (pivot, instancesAndKNNsAndDistanceToPivot) = tuple
            val instancesAndKNNs = instancesAndKNNsAndDistanceToPivot.map(secondTuple => {
                val (instance, knns, distanceToPivot) = secondTuple
                (instance, knns)
            })
            (pivot, instancesAndKNNs)
        })

        trimmedCoreKNNs.cache()

        val finalKNNs = trimmedCoreKNNs.join(finalSupportSets).map(_._2).flatMap(tuple => {
            val (instancesAndKNNs, supportSet) = tuple
            val finalKNNs = instancesAndKNNs.map(instanceAndKNeighbors => {
                val (instance, kNeighbors) = instanceAndKNeighbors
                supportSet.foreach(supportInstance => {
                    val distance = DistanceFunctions.euclidean(instance.attributes, supportInstance.attributes)
                    if(kNeighbors.contains(null) || kNeighbors.last.distance > distance){
                        addNewNeighbor(kNeighbors, new KNeighbor(supportInstance.id, distance))
                    }
                })
                (instance, kNeighbors)
            })
            finalKNNs
        })

//        finalKNNs.cache()
//        println(s"Checkpoint 1 - Cells: ${cells.partitions.length}")
//        println(s"Checkpoint 2 - CoreKNNs: ${coreKNNs.partitions.length}")
//        println(s"Checkpoint 3 - CoreDistances: ${coreDistances.partitions.length}")
//        println(s"Checkpoint 4 - SupportingDistances: ${supportingDistances.partitions.length}")
//        println(s"Checkpoint 5 - SupportingCells: ${supportingCells.partitions.length}")
//        println(s"Checkpoint 6 - PruningMeasures: ${pruningMeasures.partitions.length}")
//        println(s"Checkpoint 7 - FinalSupportSets: ${finalSupportSets.partitions.length}")
//        println(s"Checkpoint 8 - TrimmedCoreKNNs: ${trimmedCoreKNNs.partitions.length}")
//        println(s"Checkpoint 9 - FinalKNNs: ${finalKNNs.partitions.length}")
//        println(s"Executors: ${sc.getExecutorMemoryStatus}")

        finalKNNs.map(tuple => (tuple._1.id, tuple._2))
    }

    def findBasePivots(instances: RDD[Instance], sc: SparkContext): RDD[Instance] = {
        if(instances.count() == 0)
            return sc.parallelize(Seq())

        sc.parallelize(instances.takeSample(withReplacement=false, num=pivotsAmount, seed=1))
    }
}
