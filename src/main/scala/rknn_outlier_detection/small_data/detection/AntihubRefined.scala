package rknn_outlier_detection.small_data.detection

import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class AntihubRefined[A](
    val step: Double,
    val ratio: Double
) extends DetectionCriteria[A] {

    override def scoreInstances(kNeighbors: Array[Array[KNeighbor]], reverseNeighbors: Array[Array[RNeighbor]]): Array[Double] = {
        val antihubScores = new Antihub().scoreInstances(kNeighbors, reverseNeighbors)

        val neighborsScoreSum = kNeighbors.map(
            kNeighborsBatch => kNeighborsBatch
                .map(neighbor => antihubScores(neighbor.id.toInt)).sum
        )

        var finalScores = antihubScores.clone()
        var disc: Double = 0
        var i = 0
        var alpha = step * i

        while(i <= antihubScores.length && alpha <= 1){

            val newScores = antihubScores.zip(neighborsScoreSum)
                .map(tuple => findRefinedScore(tuple._1, tuple._2, alpha))

            val currentDisc = discScore(newScores, ratio)
            if(currentDisc > disc){
                finalScores = newScores.clone()
                disc = currentDisc
            }

            i += 1
            alpha = step * i
        }

        finalScores
    }

    override def scoreInstancesFromInstances(
        instances: Array[Instance[A]],
    ): Array[Double] = {

        val antihubScores = new Antihub().scoreInstancesFromInstances(instances)

        val neighborsScoreSum = instances.map(
            instance => instance.kNeighbors
                .map(neighbor => antihubScores(neighbor.id.toInt)).sum
        )

        var finalScores = antihubScores.clone()
        var disc: Double = 0
        var i = 0
        var alpha = step * i

        while(i <= antihubScores.length && alpha <= 1){

            val newScores = antihubScores.zip(neighborsScoreSum)
                .map(tuple => findRefinedScore(tuple._1, tuple._2, alpha))

            val currentDisc = discScore(newScores, ratio)
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
        val scoresCopy = scores.clone()

        scoresCopy.sortWith((score1, score2) => score1 < score2)

        val np = (scores.length * ratio).toInt

        val smallestMembers = scoresCopy.take(np)
        val uniqueItems = Set(smallestMembers)

        uniqueItems.size.toDouble / np.toDouble
    }

    def findRefinedScore(
        score: Double,
        neighborsScoreSum: Double,
        alpha: Double
    ): Double = (1 - alpha) * score + alpha * neighborsScoreSum
}
