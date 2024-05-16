package rknn_outlier_detection.big_data.search.pivot_based

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.DistanceFunction
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.exceptions.{IncorrectKValueException, IncorrectPivotsAmountException, InsufficientInstancesException}
import rknn_outlier_detection.shared.custom_objects.{DistanceObject, Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.Utils

class LAESA (
    pivotsAmount: Int,
) extends KNNSearchStrategy{

    def findBasePivots(instances: RDD[Instance], sc: SparkContext): RDD[Instance] = {
        sc.parallelize(instances.takeSample(withReplacement=false, num=pivotsAmount, seed=1))
    }

    /**
     *
     * @param instances Collection of instances to process
     * @param k Amount of neighbors for each instance
     * @param sc SparkContext of the running app
     * @return RDD containing a tuple for
     *         each instance with its array of neighbors
     */
    override def findKNeighbors(
    instances: RDD[Instance],
    k: Int,
    distanceFunction: DistanceFunction,
    sc: SparkContext
    ): RDD[(String, Array[KNeighbor])] = {

        println(s"Executors: ${sc.getExecutorMemoryStatus}")

        val instancesAmount = instances.count()

        if(instancesAmount < 2) throw new InsufficientInstancesException(s"Received less than 2 instances ($instancesAmount), not enough for a neighbors search.")
        if(pivotsAmount < 1 || pivotsAmount > instancesAmount) throw new IncorrectPivotsAmountException(s"Pivots amount ($pivotsAmount) has to be a number greater than 0 and up to the instances amount ($instancesAmount)")
        if(k <= 1 || k > instancesAmount - 1) throw new IncorrectKValueException(s"k ($k) has to be a natural number between 1 and n - 1 (n is instances length)")

        // Select base pivots
        val basePivots = findBasePivots(instances, sc)
        val basePivotsIds = basePivots.map(_.id).collect()

        // Calculate and store the distance between a pivot and every instance, for all pivots
        val pivotsDistances = basePivots.cartesian(instances)
            .filter(tuple => tuple._1.id != tuple._2.id)
            .map(tuple => {
                val (pivot, instance) = tuple
                val distance = distanceFunction(pivot.attributes, instance.attributes)
                val distanceObject = new DistanceObject(pivot.id, instance.id, distance)
                (instance , distanceObject)
            })
            .groupByKey()

        // Initialize kNeighbors with basePivots
        val kNeighbors = pivotsDistances.map(tuple => {
            val (instance, distances) = tuple
            val arr = Array.fill[KNeighbor](k)(null)
            distances.foreach(distanceObj => {
                if(distanceObj.pivotId != instance.id && (arr.contains(null) || distanceObj.distance < arr.last.distance)){
                    val newKNeighbor = new KNeighbor(distanceObj.pivotId, distanceObj.distance)
                    Utils.addNewNeighbor(arr, newKNeighbor)
                }
            })
            (instance.id, arr)
        })

        val queryWithInstanceCotas = pivotsDistances.cartesian(pivotsDistances)
            .filter(tuple => tuple._1._1.id != tuple._2._1.id)
            .map(tuple => {
                val (queryTuple, instanceTuple) = tuple
                val (query, queryDistances) = queryTuple
                val (instance, instanceDistances) = instanceTuple
                val reversedQueryDistances = queryDistances.map(distanceObj => (distanceObj.pivotId, distanceObj.distance))
                val reversedInstanceDistances = instanceDistances.map(distanceObj => (distanceObj.pivotId, distanceObj.distance))
                val allDistances = Array(reversedQueryDistances, reversedInstanceDistances).flatten
                val groupedByKey = allDistances.groupBy(tuple => tuple._1)
                val cota = groupedByKey.map(tuple => {
                    val (pivotId, distances) = tuple
                    distances.map(_._2).reduce((x, y) => math.abs(x - y))
                }).max

                (query.id, (instance, cota))
            })
            .groupByKey()

        val instancesById = instances.map(instance => (instance.id, instance))

        val allMixed = instancesById.join(queryWithInstanceCotas).join(kNeighbors).map(mixedValues => {
            val (instanceId, values) = mixedValues
            val (tuple, kNeighbors) = values
            val (query, instanceWithCotas) = tuple

            (query, kNeighbors, instanceWithCotas)
        })

        val result = allMixed.map(tuple => {
            val (query, kNeighbors, instancesWithCotas) = tuple
            instancesWithCotas.filter(t => !basePivotsIds.contains(t._1.id)).foreach(pair => {
                val (instance, cota) = pair
                if(kNeighbors.contains(null)){
                    Utils.addNewNeighbor(kNeighbors, new KNeighbor(instance.id, DistanceFunctions.euclidean(query.attributes, instance.attributes)))
                }
                else{
                    if(cota <= kNeighbors.last.distance){
                        val distance = DistanceFunctions.euclidean(query.attributes, instance.attributes)
                        if(distance < kNeighbors.last.distance){
                            Utils.addNewNeighbor(kNeighbors, new KNeighbor(instance.id, distance))
                        }
                    }
                }
            })

            (query.id, kNeighbors)
        })

        result
    }
}
