package rknn_outlier_detection.big_data.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.detection.AntihubRefined.addSmallerScore
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class AntihubRefined(params: AntihubRefinedParams) extends DetectionStrategy {

    private def discScore(scores: RDD[Double], ratio: Double): Double ={

        val np = (scores.count() * ratio).toInt

        val smallestMembers = scores.aggregate(Array.fill[Double](np)(1.0))(
            (smallestScores, nextScore) => {
                var returnScores = smallestScores
                if(nextScore < smallestScores.last){
                    returnScores = addSmallerScore(smallestScores, nextScore)
                }

                returnScores
            },
            (batch1, batch2) => {
                var returnScores = batch1
                for(score <- batch2){
                    if(score < returnScores.last){
                        returnScores = addSmallerScore(returnScores, score)
                    }
                }

                returnScores
            }
        )

        val uniqueItems = Set(smallestMembers)

        uniqueItems.size.toDouble / np.toDouble
    }

    private def findAggregateNeighborsAntihub(rNeighbors: RDD[(String, Array[RNeighbor])], antihubScores: RDD[(String, Double)]): RDD[(String, Double)] ={

        val rNeighborsAndScore = rNeighbors.join(antihubScores)
        val instanceAndKNeighborScore = rNeighborsAndScore.flatMap(instance => {
            val (_, tuple) = instance
            val (reverseNeighbors, score) = tuple
            reverseNeighbors.map(rNeighbor => (rNeighbor.id, score))
        })
//        val instanceAndGroupedKNeighborScore = instanceAndKNeighborScore.groupByKey()
//        instanceAndGroupedKNeighborScore.mapValues(_.sum)
        instanceAndKNeighborScore.reduceByKey(_ + _)
    }

    def antihubRefined(rNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] ={

        // Find antihubScores for instances and add them to corresponding instances
        val antihubScores = new Antihub().antihub(rNeighbors)
        var finalScores: RDD[(String, Double)] = findAggregateNeighborsAntihub(rNeighbors, antihubScores)

        var disc = 0.0
        var i = 0
        var alpha = params.step * i

        val joinedScoreAndAggregatesScores = antihubScores.join(finalScores)

        while(alpha <= 1){

            // Join for each instance id, the antihub value and the sum of instance neighbors antihub values
            val newScores = joinedScoreAndAggregatesScores.mapValues(tuple => {
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

    override def detect(reverseNeighbors: RDD[(String, Array[RNeighbor])]): RDD[(String, Double)] = {
        antihubRefined(reverseNeighbors)
    }
}

object AntihubRefined {
    def addSmallerScore(scores: Array[Double], newScore: Double): Array[Double] = {

        var newScoreIndex = scores.length - 1

        scores(newScoreIndex) = newScore

        while (newScoreIndex > 0 && newScore < scores(newScoreIndex - 1)) {
            scores(newScoreIndex) = scores(newScoreIndex - 1)
            newScoreIndex -= 1
            scores(newScoreIndex) = newScore
        }

        scores
    }
}
