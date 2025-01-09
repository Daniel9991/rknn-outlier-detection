import rknn_outlier_detection.shared.custom_objects.Instance
import rknn_outlier_detection.shared.distance.DistanceFunctions

package object rknn_outlier_detection {

    type Pivot = Instance
    type PivotWithCountAndDist = (Instance, Int, Double)
    type PivotWithCount = (Instance, Int)

    type DistanceFunction = (Array[Double], Array[Double]) => Double

    val euclidean: DistanceFunction = DistanceFunctions.euclidean

    val manhattan: DistanceFunction = DistanceFunctions.manhattan

    def time[T](block: => T): T = {
        val before = System.nanoTime
        val result = block
        val after = System.nanoTime
        println(s"Elapsed time: ${(after - before) / 10000000}ms")
        result
    }
}
