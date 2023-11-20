package rknn_outlier_detection.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.custom_objects.Instance

class AntihubRefined(params: AntihubRefinedParams) extends DetectionStrategy {

    def antihubRefinedFromInstances(instances: RDD[Instance]): RDD[(String, Double)] ={

        // Find antihubScores for instances and add them to corresponding instances
        val antihubScores = Antihub.detect(instances)
        val keyedInstances = instances.map(instance => (instance.id, instance))
        val scoredInstances = keyedInstances
            .join(antihubScores)
            .map(tuple => {
                val (_, tuple2) = tuple
                val (instance, antihubScore) = tuple2
                instance.antihubScore = antihubScore
                instance
            })

        var finalScores: RDD[(String, Double)] = findAggregateNeighborsAntihub(scoredInstances)

        var disc = 0.0
        var i = 0
        var alpha = params.step * i

        while(alpha <= 1){

            // Join for each instance id, the antihub value and the sum of instance neighbors antihub values
            val joinedScoreAndAggregatesScoresForInstance = antihubScores.join(finalScores)
            val newScores = joinedScoreAndAggregatesScoresForInstance.mapValues(tuple => {
                val (antihubScore, aggregateScore) = tuple
                (1 - alpha) * antihubScore + alpha * aggregateScore
            })

            // Find discrimination degree
            val currentDisc = discScore(newScores.values, params.ratio)
            if(currentDisc > disc){
                finalScores = newScores
                disc = currentDisc
            }

            i += 1
            alpha = params.step * i
        }

        finalScores
    }

    private def discScore(scores: RDD[Double], ratio: Double): Double ={

        // Why do I need to pass the function for the first argument, when it is just returning the same value
        val sortedScores = scores.sortBy(identity)

        val np = (scores.count() * ratio).toInt

        val smallestMembers = sortedScores.takeOrdered(np)
        val uniqueItems = Set(smallestMembers)

        uniqueItems.size.toDouble / np.toDouble
    }

    private def findAggregateNeighborsAntihub(instances: RDD[Instance]): RDD[(String, Double)] ={

        val instancesToAntihubScores = instances.map(instance => (instance.id, instance.antihubScore))
        val neighborsToInstances = instances.flatMap(instance => instance.kNeighbors.map(neighbor => (neighbor.id, instance.id)))
        val neighborsToGroupedInstances = neighborsToInstances.groupByKey()
        val neighborsToGroupedInstancesAndScores = neighborsToGroupedInstances
            .join(instancesToAntihubScores)
        val instancesToNeighborsScores = neighborsToGroupedInstancesAndScores.flatMap(tuple => {
            val (_, tuple2) = tuple
            val (instancesIds, score) = tuple2
            instancesIds.map(instanceId => (instanceId, score))
        })

        val instancesIdsToGroupedScores = instancesToNeighborsScores.groupByKey()
        val instancesIdsToAggregateScore = instancesIdsToGroupedScores
            .mapValues(scoresInstances => scoresInstances.sum)

        instancesIdsToAggregateScore
    }

    override def detect(instances: RDD[Instance]): RDD[(String, Double)] = {
        antihubRefinedFromInstances(instances)
    }
}
