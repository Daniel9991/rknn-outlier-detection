package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.utils.Utils
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

/*
* This class implements a knn pivot-based search method for distributed environments based
* on the following paper. https://link.springer.com/chapter/10.1007/978-3-319-71246-8_51
*/

class WIPPkNN [A](
              pivotsAmount: Int,
          ) extends KNNSearchStrategy[A] {

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

    override def findKNeighbors(instances: RDD[Instance[A]], k: Int, distanceFunction: DistanceFunction[A], sc: SparkContext): RDD[(String, Array[KNeighbor])] = {

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
        val supportingCells1 = supportingDistances.cartesian(cells)
            .filter(tuple => tuple._1._1.id != tuple._2._1.id)
            .groupByKey()
            .map(tuple => {
                val (cellAndSupportDistance, allOtherCells) = tuple
                val (cell, supportDistance) = cellAndSupportDistance

                // TODO Iterable has no filter method? Need to find array alternative
                val supportCells = allOtherCells
                    .toArray
                    .filter(otherCell => {
                        val (cellPivot, _) = otherCell
                        distanceFunction(cell.attributes, cellPivot.attributes) / 2 <= supportDistance
                    })
                    .flatMap(tuple => tuple._2)

                (cell, supportCells)
            })

        val supportCells = supportingDistances.cartesian(cells)
            .filter(tuple => {
                tuple._1._1.id != tuple._2._1.id &&
                    distanceFunction(tuple._1._1.attributes, tuple._2._1.attributes) / 2 <= tuple._1._2
            })
            .flatMapValues(otherCell => otherCell._2).map(tuple => (tuple._1._1, tuple._2))

        supportCells.cache()

        // Step 5
        val pruningMeasurements = supportCells.map(cellAndSupportInstance => {
            val (pivot, supportInstanceAndDist) = cellAndSupportInstance
            val (supportInstance, distanceToItsPivot) = supportInstanceAndDist
            val pruneValue = math.abs(distanceFunction(pivot.attributes, supportInstance.attributes) - distanceToItsPivot) / 2
            (pivot, (supportInstance, pruneValue))
        })

        val finalSupportSets = coreDistances.join(pruningMeasurements)
            .filter(tuple => tuple._2._2._2 < tuple._2._1)
            .map(tuple => {
                val pivot = tuple._1
                val supportInstance = tuple._2._2._1
                (pivot, new KNeighbor(supportInstance.id, distanceFunction(pivot.attributes, supportInstance.attributes)
                ))
            })

        finalSupportSets.cache()

        // Step 6

        val supportKNNS = finalSupportSets.aggregateByKey(Array.fill[KNeighbor](k)(null))(
            (acc, neighbor) => {
                var finalAcc = acc
                if(acc.last == null || neighbor.distance < acc.last.distance)
                    finalAcc = Utils.insertNeighborInArray(acc, neighbor)

                finalAcc
            },
            (acc1, acc2) => {
                var finalAcc = acc1
                for(neighbor <- acc2){
                    if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                        finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                    }
                }

                finalAcc
            })

        val finalKNNs = coreKNNs.map(tuple => (tuple._1, tuple._2.flatMap(_._2).toArray))
            .join(supportKNNS)
            .map(tuple => {
                val (instance, neighbors) = tuple
                val (coreNeighbors, supportNeighbors) = neighbors
                var finalAcc = coreNeighbors
                for(neighbor <- supportNeighbors){
                    if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                        finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                    }
                }

                (instance.id, finalAcc)
            })

        finalKNNs
    }

    def findBasePivots(instances: RDD[Instance[A]], sc: SparkContext): RDD[Instance[A]] = {
        if(instances.count() == 0)
            return sc.parallelize(Seq())

        sc.parallelize(instances.takeSample(withReplacement=false, num=pivotsAmount, seed=1))
    }
}
