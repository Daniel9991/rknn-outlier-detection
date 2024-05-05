//package rknn_outlier_detection
//
//import org.apache.spark.sql.SparkSession
//import rknn_outlier_detection.shared.custom_objects.Instance
//import rknn_outlier_detection.shared.utils.ReaderWriter
//import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
//
//object DetectionExperiment {
//
//    def main(args: Array[String]): Unit = {
//
////        val filename = "testingDatasets/creditcardMinMaxScaled.csv"
//        val filename = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\datasets\\iris-synthetic.csv"
//
//
//        val spark:SparkSession = SparkSession
//            .builder()
//            .master("spark://127.0.0.1:7077")
//            .appName("rknn-outlier-detection-cluster")
//            .getOrCreate()
//
//        val rawData = ReaderWriter.readCSV(filename, hasHeader=false)
//        val instances = rawData.zipWithIndex.map(tuple => {
//            val (line, index) = tuple
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            new Instance(index.toString, attributes, classification="")
//        })
//
//        val neighbors = ExhaustiveBigData.findKNeighbors(
//            spark.sparkContext.parallelize(instances),
//            20,
//            spark.sparkContext
//        )
//
//        val stringifiedNeighbors = neighbors.map(tuple => {
//
//                val x = s"${tuple._1},${tuple._2.map(neighbor => s"${neighbor.id};${neighbor.distance}").mkString(",")}"
//                x
//            }
//        )
//
////        val df = spark.createDataFrame(neighbors)
//
//        val toWrite = stringifiedNeighbors.collect().mkString("\n")
//        println(toWrite)
//        ReaderWriter.writeToFile(
//            "testingDatasets/creditcardMinMaxScaledNeighbors.csv",
//            toWrite
//        )
//    }
//
////    def main(args: Array[String]): Unit = {
////
////        val filename = "testingDatasets/creditcardMinMaxScaled.csv"
////
////        val spark:SparkSession = SparkSession
////            .builder()
////            .master("local[*]")
////            .appName("myapp")
////            .getOrCreate()
////
////        val rawData = ReaderWriter.readCSV(filename, hasHeader=false)
////        val instances = rawData.zipWithIndex.map(tuple => {
////            val (line, index) = tuple
////            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
////            new Instance(index.toString, attributes, classification="")
////        })
////
////        val detector = new Detector(
////            searchStrategy = ExhaustiveSearch,
////            detectionStrategy = new AntihubRefined(new AntihubRefinedParams(0.1, 0.3)),
////            classificationStrategy = new TopN(500),
////            normalLabel = "normal",
////            outlierLabel = "outlier",
////            spark.sparkContext
////        )
////
////        val results = detector.detectOutliers(
////            spark.sparkContext.parallelize(instances),
////            15
////        )
////
////        ReaderWriter.writeToFile(
////            "testingDatasets/result.csv",
////            results.collect().map(tuple => s"${tuple._1},${tuple._2}").mkString("\n")
////        )
////    }
//}
