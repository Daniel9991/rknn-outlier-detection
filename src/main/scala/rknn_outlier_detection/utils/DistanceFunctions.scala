package rknn_outlier_detection.utils

object DistanceFunctions {

    def euclidean(instance1: Array[Double], instance2: Array[Double]): Double = {

        if (instance1.length != instance2.length)
            throw new Exception("Instances have different data length")

        var sum: Double = 0

        for ((from1, from2) <- instance1.zip(instance2)) {
            sum += math.pow(from1 - from2, 2)
        }

        math.sqrt(sum)
    }
}
