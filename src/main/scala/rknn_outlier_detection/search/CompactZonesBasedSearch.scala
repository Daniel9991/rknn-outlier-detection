package rknn_outlier_detection.search
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.{Instance, KNeighbor, Neighbor, Neighborhood}
import rknn_outlier_detection.distance.DistanceFunctions
import rknn_outlier_detection.search.CompactZonesBasedSearch.findNeighborhoods
import rknn_outlier_detection.utils.Utils

import scala.util.control.Breaks.break

class CompactZonesBasedSearch extends KNNSearchStrategy {

    val PIVOT_SAMPLING_SEED = 384
    val DATASET_FRACTION_FOR_PIVOTS = 0.0001 // 1 pivot per 1000 instances

    override def findKNeighbors(instances: RDD[Instance], k: Integer, sc: SparkContext): RDD[(String, Array[KNeighbor])] = {

        // TODO: Clean/Optimize this up

        // Find pivots
        val pivots = instances.sample(withReplacement = false, DATASET_FRACTION_FOR_PIVOTS, PIVOT_SAMPLING_SEED)

        // Find neighborhoods
        val neighborhoods = findNeighborhoods(instances, pivots, sc)

        // Perform search in neighborhoods
        val instanceIdsToKNeighbors = neighborhoods.flatMap(neighborhood => {
            neighborhood.instances
                .cartesian(neighborhood.instances)
                .filter(pair => pair._1.id != pair._2.id)
                .map(pair => {
                    val (ins1, ins2) = pair
                    (
                        ins1.id,
                        new KNeighbor(
                            ins2.id,
                            DistanceFunctions.euclidean(ins1.attributes, ins2.attributes)
                        )
                    )
                })
                .aggregateByKey(Array.fill[KNeighbor](k)(null))(
                    (acc, neighbor) => {
                        if(acc.last == null || neighbor.distance < acc.last.distance)
                            Utils.insertNeighborInArray(acc, neighbor)

                        acc
                    },
                    (acc1, acc2) => {
                        var finalAcc = acc1
                        for(neighbor <- acc2){
                            if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                                finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                            }
                            else{
                                break
                            }
                        }

                        finalAcc
                    }
                ).collect()
        })

        // Perform rectification????

        instanceIdsToKNeighbors
    }
}

object CompactZonesBasedSearch{

    // TODO: Clean/Optimize this up
    def findNeighborhoods(instances: RDD[Instance], pivots: RDD[Instance], sc: SparkContext): RDD[Neighborhood] = {
        // Pairs of pivot-instance where instance is both are not being filtered out so that the pivot is part
        // of the instances for its neighborhood
        val instancesToPivotsPairs = instances.cartesian(pivots)

        val groupedPivotsForInstance = instancesToPivotsPairs.groupBy(_._1).map(tuple => (tuple._1, tuple._2.map(_._2)))

        val instancesToClosestPivot = groupedPivotsForInstance.map(instanceToPivots => {
            val (instance, pivots) = instanceToPivots

            var closestDistance = Double.PositiveInfinity
            var closestPivot = null.asInstanceOf[Instance]

            val distanceToPivotIter = pivots.map(pivot =>
                (DistanceFunctions.euclidean(instance.attributes, pivot.attributes), pivot)
            )

            distanceToPivotIter.foreach(tuple => {
                if(tuple._1 < closestDistance){
                    closestDistance = tuple._1
                    closestPivot = tuple._2
                }
            })

            (instance, closestPivot)
        })

        instancesToClosestPivot.groupBy(_._2).map(tuple => new Neighborhood(tuple._1, sc.parallelize(tuple._2.map(tuple => tuple._2).toSeq)))
    }
}