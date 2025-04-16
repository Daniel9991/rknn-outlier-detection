package rknn_outlier_detection.shared.distance

object DistanceFunctions {

    def euclidean(instance1: Array[Double], instance2: Array[Double]): Double = {

        if (instance1.length != instance2.length)
            throw new Exception("Instances have different data length")

        var sum: Double = 0

        for ((from1, from2) <- instance1.zip(instance2)) {
            sum += math.pow(from1 - from2, 2)
        }

        BigDecimal(math.sqrt(sum)).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    def manhattan(instance1: Array[Double], instance2: Array[Double]): Double = {

        if (instance1.length != instance2.length)
            throw new Exception("Instances have different data length")

        var sum: Double = 0

        for ((from1, from2) <- instance1.zip(instance2)) {
            sum += math.abs(from1 - from2)
        }

        sum
    }
}
