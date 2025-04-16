package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import rknn_outlier_detection.BigDataExperiment.saveStatistics
import rknn_outlier_detection.big_data.alternative_methods.SameThingByPartition
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.big_data.search.pivot_based.{FarthestFirstTraversal, IncrementalSelection, PkNN}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.detection
import rknn_outlier_detection.small_data.search.ExhaustiveSmallData

object PkNNExperiments {
    def main(args: Array[String]): Unit = {
//        test(args)
        rocExperiment(args)
    }

    def rocExperiment(args: Array[String]): Unit = {

        val datetime = java.time.LocalDateTime.now()
        val dateString = formatLocalDateTime(datetime)
        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 142
        val k = if(args.length > 2) args(2).toInt else 200
        val seed = if(args.length > 3) args(3).toInt else 6745
        val datasetSize = if(args.length > 4) args(4).toInt else -1
        val detectionMethod = if(args.length > 5) args(5) else "antihub"
        val pivotStrategy = if(args.length > 6) args(6) else "random"
        val distanceFunction = euclidean

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf()
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))
            config.setMaster("local[*]")
            config.set("spark.default.parallelism", "72")
            config.set("spark.driver.memory", "8g")
            config.set("spark.executor.memory", "4g")
            config.set("spark.default.parallelism", "72")

            val spark = SparkSession.builder()
                .config(config)
                .appName(s"Test k: $k seed: $seed method: $detectionMethod")
                .getOrCreate();

            val sc = spark.sparkContext

            import spark.implicits._

            val detectionCriteria: rknn_outlier_detection.small_data.detection.DetectionStrategy = detectionMethod match {
                case "antihub" => new detection.Antihub()
                case "ranked" => new detection.RankedReverseCount(k, 0.7)
                case "refined" => new detection.AntihubRefined(0.1, 0.3)
            }

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.persist()

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            if(pivotStrategy != "random" && pivotStrategy != "fft" && pivotStrategy != "is"){
                throw new Exception(s"Unknown pivot selection strategy: $pivotStrategy")
            }

            val onPivotStart = System.nanoTime()
            val selectedPivots = pivotStrategy match {
                case "random" => instances.takeSample(withReplacement = false, pivotsAmount, seed)
                case "fft" =>
                    val objectSet = sc.parallelize(instances.takeSample(withReplacement = false, seed=seed, num=6190), sc.defaultParallelism)
                    new FarthestFirstTraversal(seed=seed).findPivots(objectSet, pivotsAmount, distanceFunction)
                case "is" =>
                    val sample = instances.takeSample(withReplacement=false, num=2500, seed=seed)
                    val candidatePivots = sc.parallelize(sample.slice(0, 500))
                    val objectPairs = sc.parallelize(sample.slice(500, 1500).zip(sample.slice(1500, 2500)))
                    new IncrementalSelection().findPivots(candidatePivots, objectPairs, pivotsAmount, distanceFunction, sc)
            }
            val onPivotEnd = System.nanoTime()

            val onStartPkNN = System.nanoTime()
            val outlierDegreesPkNN = new PkNN(Array(), 0).pknnAllTheWay(instances, selectedPivots, k, distanceFunction, "antihub", sc).persist(StorageLevel.MEMORY_AND_DISK_SER)
            outlierDegreesPkNN.count()
            val onFinishPkNN = System.nanoTime
            val durationPkNN = (onFinishPkNN - onStartPkNN) / 1000000
            val predictionsAndLabelsPkNN = classifications.join(outlierDegreesPkNN).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsPkNN = new BinaryClassificationMetrics(predictionsAndLabelsPkNN)
            val rocPkNN = detectionMetricsPkNN.areaUnderROC()
            outlierDegreesPkNN.unpersist()

            val onStartByPartition = System.nanoTime()
            val outlierDegreesByPartition = new SameThingByPartition().detectAnomalies(instances, pivotsAmount, seed, k, distanceFunction, sc, detectionCriteria, selectedPivots).persist(StorageLevel.MEMORY_AND_DISK_SER)
            outlierDegreesByPartition.count()
            val onFinishByPartition = System.nanoTime
            val durationByPartition = (onFinishByPartition - onStartByPartition) / 1000000
            val predictionsAndLabelsByPartition = classifications.join(outlierDegreesByPartition).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsByPartition = new BinaryClassificationMetrics(predictionsAndLabelsByPartition)
            val rocByPartition = detectionMetricsByPartition.areaUnderROC()


            println(s"By Partition ROC: $rocByPartition. By Partition duration: $durationByPartition. PkNN ROC: $rocPkNN. PkNN duration: $durationPkNN.")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                System.in.read()
            }
        }
    }

    def test(args: Array[String]): Unit = {

        val fullPath = System.getProperty("user.dir")
        val k = 50
        val pivotsAmount = 40

        val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_20000.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val rawData = ReaderWriter.readCSV(datasetPath, hasHeader = false)
        val instancesAndClassification = rawData.zipWithIndex.map { case (line, index) =>
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            val classification = if (line.last == "1") "1.0" else "0.0"
            (Instance(index, attributes), classification)
        }
        val instances = instancesAndClassification.map(_._1)

        val smallSearcher = new ExhaustiveSmallData()
        val smallStart = System.nanoTime()
        val smallKNNs = instances.zipWithIndex.map{case (instance, index) => {
            println(s"Processing instance ${index}")
            (instance.id, smallSearcher.findQueryKNeighbors(instance, instances, k, euclidean))
        }}
        val smallEnd = System.nanoTime()
        val mappedKNNs = Map.from(smallKNNs)

        val config = new SparkConf()
        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))
        config.setMaster("local[*]")
//        config.set("spark.default.parallelism", "128")

        val spark = SparkSession.builder()
            .config(config)
            .appName(s"Test pknn")
            .getOrCreate();

        val sc = spark.sparkContext

        val distributedInstances = sc.parallelize(instances, 16)
        val sample = distributedInstances.takeSample(withReplacement = false, pivotsAmount, 998324)
        val pknn = new PkNN(Array(), 1)
        val startDistributed = System.nanoTime()
        val distributedKNNs = pknn.mypknn(distributedInstances, sample, k, euclidean, sc).persist(StorageLevel.MEMORY_AND_DISK_SER)
        distributedKNNs.count()
        val endDistributed = System.nanoTime()

        val ok = distributedKNNs.collect.map{case (id, kNeighbors) => {
            val smallResult = mappedKNNs(id)
            smallResult.zip(kNeighbors).map{case (smallNeighbor, distributedNeighbor) =>
                smallNeighbor.id == distributedNeighbor.id || smallNeighbor.distance == distributedNeighbor.distance
            }.forall(identity)
        }}.forall(identity)

        println(s"Was ok: $ok. Small: ${(smallEnd - smallStart) / 1000000}. Distributed: ${(endDistributed - startDistributed) / 1000000}")

    }

}
