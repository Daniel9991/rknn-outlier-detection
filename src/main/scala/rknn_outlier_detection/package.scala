import rknn_outlier_detection.shared.distance.DistanceFunctions

package object rknn_outlier_detection {
    type DistanceFunction[A] = (A, A) => Double
    val euclidean: DistanceFunction[Array[Double]] = DistanceFunctions.euclidean
    val manhattan: DistanceFunction[Array[Double]] = DistanceFunctions.manhattan
    def time[T](block: => T): T = {
        val before = System.nanoTime
        val result = block
        val after = System.nanoTime
        println(s"Elapsed time: ${(after - before) / 1000000}ms")
        result
    }
}
