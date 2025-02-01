package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

import scala.collection.immutable.HashMap

class AntihubRefined(
    val step: Double,
    val ratio: Double
) extends DetectionCriteria {

    override def scoreInstances(reverseNeighbors: Array[(Int, Array[RNeighbor])]): Array[(Int, Double)] = {
        val antihubScores = new Antihub().scoreInstances(reverseNeighbors)
        val antihubScoresMap = HashMap.from(antihubScores)

        val instanceAndKNeighborsScore = reverseNeighbors
            .flatMap{case (id, reverseNeighbors) =>
                reverseNeighbors.map(rNeighbor => (rNeighbor.id, antihubScoresMap(id)))
            }
            .groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray
            .map{case (instanceId, neighborsAntihubs) => (instanceId, neighborsAntihubs.sum)}

        val scoresJoin = instanceAndKNeighborsScore.map{case (instanceId, kNeighborsScoreSum) => (instanceId, kNeighborsScoreSum, antihubScoresMap(instanceId))}

        var finalScores = instanceAndKNeighborsScore.clone()
        var disc: Double = 0
        var i = 0
        var alpha = step * i

        while(alpha <= 1){

            val newScores = scoresJoin
                .map{case (instanceId, kNeighborsScoreSum, antihub) => (instanceId, findRefinedScore(antihub, kNeighborsScoreSum, alpha))}

            val currentDisc = discScore(newScores.map(_._2), ratio)
            if(currentDisc > disc){
                finalScores = newScores.clone()
                disc = currentDisc
            }

            i += 1
            alpha = step * i
        }

        finalScores
    }

    def discScore(scores: Array[Double], ratio: Double): Double = {
        val sortedScores = scores.sortWith((score1, score2) => score1 < score2)

        val np = (scores.length * ratio).toInt

        val smallestMembers = sortedScores.take(np)
        val uniqueItems = Set.from(smallestMembers)

        uniqueItems.size.toDouble / np.toDouble
    }

    def findRefinedScore(
        score: Double,
        neighborsScoreSum: Double,
        alpha: Double
    ): Double = (1 - alpha) * score + alpha * neighborsScoreSum
}
