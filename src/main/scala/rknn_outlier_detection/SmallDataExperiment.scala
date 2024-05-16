package rknn_outlier_detection

import rknn_outlier_detection.shared.custom_objects.Instance
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.detection.{Antihub, AntihubRefined, DetectionCriteria, RankedReverseCount}
import rknn_outlier_detection.small_data.search.{ExhaustiveSmallData, KNNSearchStrategy, ReverseNeighborsSmallData}
import rknn_outlier_detection.small_data.search.pivot_based.LAESA

object SmallDataExperiment {
    def main(args: Array[String]): Unit = {

    }

    def runSmallDataExperiment(): Unit ={
        def instancesLoader(rawData: Array[Array[String]]) = {
            rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "Iris-setosa") "1.0" else "0.0"
                new Instance(index.toString, attributes, classification=classification)
            })
        }

        val distFun: DistanceFunction = DistanceFunctions.euclidean
        val kValues = Array(15, 25, 35)
        val smallDataCombinations: Array[(String, Boolean, String, Array[Array[String]] => Array[Instance], KNNSearchStrategy, DetectionCriteria)] = Array(
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with exhaustive search and Antihub criteria", instancesLoader, ExhaustiveSmallData, Antihub),
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with exhaustive search and AntihubRefined criteria", instancesLoader, ExhaustiveSmallData, new AntihubRefined(0.2, 0.3)),
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with exhaustive search and RankedReverseCount criteria", instancesLoader, ExhaustiveSmallData, RankedReverseCount),
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with LAESA search and Antihub criteria", instancesLoader, new LAESA(7), Antihub),
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with LAESA search and AntihubRefined criteria", instancesLoader, new LAESA(7), new AntihubRefined(0.2, 0.3)),
            ("datasets/iris-synthetic-2.csv", false, "Testing small data with LAESA search and RankedReverseCount  criteria", instancesLoader, new LAESA(7), RankedReverseCount),
        )

        smallDataCombinations.foreach(tuple => {
            val (filename, hasHeader, heading, instancesLoader, searchStrategy, detectionCriteria) = tuple

            kValues.foreach(kValue => {
                println(s"$heading for k=$kValue")
                time(smallDataDetectionFlow(filename, hasHeader, instancesLoader, searchStrategy, kValue, distFun, detectionCriteria))
            })
        })
    }

    def smallDataDetectionFlow(
                                  datasetFilename: String,
                                  hasHeader: Boolean,
                                  instancesLoader: Array[Array[String]] => Array[Instance],
                                  searchStrategy: KNNSearchStrategy,
                                  k: Int,
                                  distanceFunction: DistanceFunction,
                                  detectionCriteria: DetectionCriteria
                              ): Unit = {
        val rawData = ReaderWriter.readCSV(datasetFilename, hasHeader=hasHeader)
        val instances = instancesLoader(rawData)
        val kNeighbors = searchStrategy.findKNeighbors(instances, k, distanceFunction)
        val reverseNeighbors = ReverseNeighborsSmallData.findReverseNeighbors(kNeighbors)
        val detection = detectionCriteria.scoreInstances(kNeighbors, reverseNeighbors)
    }
}
