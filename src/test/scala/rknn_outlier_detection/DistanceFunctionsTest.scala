package rknn_outlier_detection

import org.scalatest.funsuite.AnyFunSuite
import rknn_outlier_detection.shared.distance.DistanceFunctions

class DistanceFunctionsTest extends AnyFunSuite{
    test("DistanceFunctions.euclidean") {
        assert(DistanceFunctions.euclidean(Array(1, 2), Array(1, 2)) === 0)
    }

}
