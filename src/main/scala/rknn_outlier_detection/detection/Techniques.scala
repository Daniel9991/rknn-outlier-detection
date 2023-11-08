package rknn_outlier_detection.detection

    import org.apache.spark.rdd.RDD
    import rknn_outlier_detection.custom_objects.{Instance, Neighbor}

object Techniques {

    def normalizeReverseNeighborsCount(count: Int): Double = {
        if(count == 0)
            1.0
        else
            1.0 / count.toDouble
    }

    def antihubFromInstances(instances: RDD[Instance]): RDD[(String, Double)] ={
        instances.map(instance => (instance.id, normalizeReverseNeighborsCount(instance.rNeighbors.length)))
    }

    def antihub(idsWithRNeighbors: RDD[(String, Array[Neighbor])]): RDD[(String, Double)] ={
        idsWithRNeighbors.map(tuple => (tuple._1, normalizeReverseNeighborsCount(tuple._2.length)))
    }

    def antihubRefinedFromInstances(instances: RDD[Instance], step: Double, ratio: Double): RDD[(String, Double)] ={

        val antihubScores = antihubFromInstances(instances)

        instances.map(instance => ("", 0))
    }


//    def scoreInstances(
//                          instances: Array[Instance],
//                      ): Array[Double] = {
//
//        val step = 0.1
//        val ratio = 0.3
//
//        val antihubScores = Antihub.scoreInstances(instances)
//
//        val neighborsScoreSum = instances.map(
//            instance => instance.kNeighbors
//                .map(neighbor => antihubScores(neighbor.index)).sum
//        )
//
//        var finalScores = antihubScores.clone()
//        var disc: Double = 0
//        var i = 0
//        var alpha = step * i
//
//        while(i <= antihubScores.length && alpha <= 1){
//
//            val newScores = antihubScores.zip(neighborsScoreSum)
//                .map(tuple => findRefinedScore(tuple._1, tuple._2, alpha))
//
//            val currentDisc = discScore(newScores, ratio)
//            if(currentDisc > disc){
//                finalScores = newScores.clone()
//                disc = currentDisc
//            }
//
//            i += 1
//            alpha = step * i
//        }
//
//        finalScores
//    }
//
//    def discScore(scores: Array[Double], ratio: Double): Double = {
//        val scoresCopy = scores.clone()
//
//        scoresCopy.sortWith((score1, score2) => score1 < score2)
//
//        val np = (scores.length * ratio).toInt
//
//        val smallestMembers = scoresCopy.take(np)
//        val uniqueItems = Set(smallestMembers)
//
//        uniqueItems.size.toDouble / np.toDouble
//    }
//
//    def findRefinedScore(
//                            score: Double,
//                            neighborsScoreSum: Double,
//                            alpha: Double
//                        ): Double = (1 - alpha) * score + alpha * neighborsScoreSum
}
