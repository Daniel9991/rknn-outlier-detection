package rknn_outlier_detection.big_data.detection
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.big_data.detection.AntihubRefined.addSmallerScore
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

class AntihubRefined(ratio: Double, step: Double) extends DetectionStrategy with Serializable {

    private def discScore(scores: RDD[Double], ratio: Double): Double ={

        val np = (scores.count() * ratio).toInt

        val smallestMembers = scores.aggregate(Array.fill[Double](np)(Double.PositiveInfinity))(
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

    private def findAggregateNeighborsAntihub(rNeighbors: RDD[(Int, Array[RNeighbor])], antihubScores: RDD[(Int, Double)]): RDD[(Int, Double)] ={

        val rNeighborsAndScore = rNeighbors.join(antihubScores) // Sustituible por zip
        val instanceAndKNeighborScore = rNeighborsAndScore.flatMap{case (id, (reverseNeighbors, antihubScore)) =>
            reverseNeighbors.map(rNeighbor => (rNeighbor.id, antihubScore))
        }
        instanceAndKNeighborScore.reduceByKey(_+_)
    }

    def antihubRefined(rNeighbors: RDD[(Int, Array[RNeighbor])], antihubScores: RDD[(Int, Double)]): RDD[(Int, Double)] ={

        // Find antihubScores for instances and add them to corresponding instances
        var finalScores: RDD[(Int, Double)] = findAggregateNeighborsAntihub(rNeighbors, antihubScores)

        var disc = 0.0
        var i = 0
        var alpha = step * i

        val joinedScoreAndAggregatesScores = antihubScores.join(finalScores).cache() // Sustituible por zip

        while(alpha <= 1){

            // Join for each instance id, the antihub value and the sum of instance neighbors antihub values
            val newScores = joinedScoreAndAggregatesScores.mapValues{
                case (antihubScore, aggregateScore) =>
                    (1 - alpha) * antihubScore + alpha * aggregateScore
            }

            // Calculate discrimination degree
            val currentDisc = discScore(newScores.values, ratio)
            if(currentDisc > disc){
                finalScores = newScores
                disc = currentDisc
            }

            i += 1
            alpha = step * i
        }

        joinedScoreAndAggregatesScores.unpersist()
        finalScores
    }

    // Version used by detect method from DetectStrategy trait
    def antihubRefinedDetect(rNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] ={

        val antihubScores = new Antihub().antihub(rNeighbors)

        // Find antihubScores for instances and add them to corresponding instances
//        var finalScores: RDD[(Int, Double)] = findAggregateNeighborsAntihub(rNeighbors, antihubScores)
        var aggregateScores: RDD[(Int, Double)] = findAggregateNeighborsAntihub(rNeighbors, antihubScores)
        var finalScores: RDD[(Int, Double)] = antihubScores.map(identity)

        var disc = 0.0
        var i = 0
        var alpha = step * i

        val joinedScoreAndAggregatesScores = antihubScores.join(aggregateScores).cache()

        while(alpha <= 1){

            // Join for each instance id, the antihub value and the sum of instance neighbors antihub values
            val newScores = joinedScoreAndAggregatesScores.mapValues{
                case (antihubScore, aggregateScore) =>
                    (1 - alpha) * antihubScore + alpha * aggregateScore
            }

            // Calculate discrimination degree
            if(finalScores.collect.exists(el => el._2 > 1.78)){
                val justBecause = finalScores.collect()
                println("found")
            }
            val currentDisc = discScore(newScores.values, ratio)
            if(currentDisc > disc){
                if(i != 0){
                    println("")
                }
                finalScores = newScores.map(identity)
                disc = currentDisc
            }

            i += 1
            alpha = step * i
        }

        joinedScoreAndAggregatesScores.unpersist()
        val x = finalScores.collect()
        val y = 1
        finalScores
    }

    def detect(reverseNeighbors: RDD[(Int, Array[RNeighbor])]): RDD[(Int, Double)] = {
        antihubRefinedDetect(reverseNeighbors)
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
