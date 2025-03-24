package rknn_outlier_detection

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.{ReaderWriter, Utils}
import rknn_outlier_detection.big_data.search.exhaustive_knn.ExhaustiveBigData
import rknn_outlier_detection.shared.distance.DistanceFunctions
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, SparkSession}
import rknn_outlier_detection.SmallDataExperiment.saveStatistics
import rknn_outlier_detection.big_data.alternative_methods.SameThingByPartition
import rknn_outlier_detection.big_data.detection.{Antihub, AntihubRefined, DetectionStrategy, RankedReverseCount}
import rknn_outlier_detection.big_data.full_implementation.{Antihub, StructuredAntihub}
import rknn_outlier_detection.big_data.partitioners.PivotsPartitioner
import rknn_outlier_detection.big_data.search.KNNSearchStrategy
import rknn_outlier_detection.big_data.search.pivot_based.{FarthestFirstTraversal, GroupedByPivot, IncrementalSelection, PkNN}
import rknn_outlier_detection.big_data.search.reverse_knn.NeighborsReverser
import rknn_outlier_detection.shared.utils.Utils.addNewNeighbor
import rknn_outlier_detection.small_data.{SmallDataDetector, detection}
import rknn_outlier_detection.small_data.detection.DetectionStrategy
import rknn_outlier_detection.small_data.search.{ExhaustiveSmallData, ReverseNeighborsSmallData}

import scala.annotation.tailrec
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object BigDataExperiment {

    def main(args: Array[String]): Unit = {
//        mainExperiment(args)
//        byPartitionExperiment(args)
        byPartitionFullLengthExperiment(args)
//        smallDataExperiment(args)

//        val fullPath = System.getProperty("user.dir")
//
//        val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled.csv"
//        val datasetPath = s"${fullPath}\\${datasetRelativePath}"
//
//        val config = new SparkConf().setAppName("Scaled creditcard test")
//        config.setMaster("local[*]")
//        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//        config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))
//
//        val spark = SparkSession.builder()
//            .config(config)
//            .appName("Scaled creditcard test")
//            .getOrCreate();
//
//        val sc = spark.sparkContext
//
//        import spark.implicits._
//
//        val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
//        val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            val classification = if (line.last == "1") "1.0" else "0.0"
//            (Instance(index.toInt, attributes), classification)
//        }}
//
//        val instances = instancesAndClassification.map(_._1).take(10)
//
//        val exhaustiveSmallData = new ExhaustiveSmallData()
//        val kNeighbors = instances.map(instance => (instance.id, exhaustiveSmallData.findQueryKNeighbors(instance, instances, 3, euclidean)))
//        val filteredKNeighbors = kNeighbors.map{case (id, neighbors) => (id, neighbors.filter(n => n != null))}
//        val rNeighbors = filteredKNeighbors.flatMap{case (instanceId, kNeighbors) =>
//            kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
//        }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray
//
//        val instancesSet = HashSet.from(instances.map(_.id))
//        val rNeighborsSet = HashSet.from(rNeighbors.map(_._1))
//        val notFound = instancesSet.diff(rNeighborsSet)
//        val notFoundRNeighbors = notFound.map(id => (id, Array.empty[RNeighbor]))
//        val fullRNeighbors = rNeighbors.concat(notFoundRNeighbors)
//
//        val refinedRDD = new AntihubRefined(0.3, 0.1).antihubRefinedDetect(sc.parallelize(fullRNeighbors)).cache()
//        val refined = new detection.AntihubRefined(0.1, 0.3).scoreInstances(fullRNeighbors)
//
//        val joinedScores = refinedRDD.join(sc.parallelize(refined)).collect()
//        println(joinedScores.map{case (id, (scoreRDD, regularScore)) => s"$id: ($scoreRDD, $regularScore)"}.mkString("\n", "\n", "\n"))
    }

//    def compareReverseNeighborsCountBetweenApproximateAndExactSearch(): Unit ={
//        val exactKNeighborsPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results-fixed.csv"
//        val sc = new SparkContext(new SparkConf()
//            .setMaster("local[*]")
//            .setAppName("Scaled creditcard test")
//            .set("spark.default.parallelism", "64")
//            .set("spark.executor.memory", "12g")
//        )
//
//        val pathToCurrentDir = System.getProperty("user.dir")
//        val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
//        val datasetPath = s"${pathToCurrentDir}\\${datasetRelativePath}"
//
//        try {
//
//            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
//            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
//                val (line, index) = tuple
//                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//                val classification = if (line.last == "1") "1.0" else "0.0"
//                (new Instance(index.toInt, attributes), classification)
//            }).cache()
//            val instances = instancesAndClassification.map(_._1)
//            val idAndClassification = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}
//            val outliersIds = idAndClassification
//                .filter{case (_, classification) => classification == "1.0"}
//                .map(_._1)
//                .collect
//
////            val rawKNeighbors = sc.textFile(exactKNeighborsPath).map(line => line.split(","))
////            val exactKNeighbors = rawKNeighbors
////                .map(line => {
////                    val id = line(0).toInt
////                    val neighbors = line.slice(1, line.length).map(token => {
////                        val tokenParts = token.split("::")
////                        if (tokenParts.length < 2) {
////                            throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
////                        }
////                        new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
////                    })
////
////                    (id, neighbors)
////                })
////                .filter{case (id, kNeighbors) => outliersIds.contains(id)}
////                .map{case (id, kNeighbors) => (id, kNeighbors.slice(0, 800))}
////
////            val exactRNeighbors = NeighborsReverser.findReverseNeighbors(exactKNeighbors)
////                .collect()
////                .sortBy(_._1)
////                .map{case (id, rNeighbors) => s"$id,${rNeighbors.length}"}
//
//
//
//
//
//            val k = 800
//            val seed = 57124
//            val pivotsAmount = 25
//            val distanceFunction = euclidean
//            val _pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed) // seed=87654 seed2=458212 seed3=57124
//            val pivots = sc.broadcast(_pivots)
//
//            // Create cells
//            val cells = instances.map(instance => {
//                val closestPivot = pivots.value
//                    .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
//                    .reduce{(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2}
//
//                (closestPivot._1, instance)
//            }).cache()
//
//            println(cells.groupByKey().mapValues(_.toArray.map(_.id)).filter(tuple => tuple._2.contains(21564)).map(t => t._2.length).collect.mkString("\n"))
//
//            return
//
//            val incompletePivots = Array(4172, 24145, 40673, 46821, 1388, 29052)
////            println(cells.groupByKey().map{case (pivot, iter) => s"${pivot.id}: ${iter.toArray.length} points"}.collect.mkString("\n"))
//
//            //        val repartitioned = cells.repartition(cells.getNumPartitions).cache()
//            val coreKNNs = cells.join(cells)
//                .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
//                .map(tuple => (tuple._2._1, new KNeighbor(tuple._2._2.id, distanceFunction(tuple._2._1.data, tuple._2._2.data))))
//                .aggregateByKey(Array.fill[KNeighbor](k)(null))(
//                    (acc, neighbor) => {
//                        var finalAcc = acc
//                        if(acc.last == null || neighbor.distance < acc.last.distance)
//                            finalAcc = Utils.insertNeighborInArray(acc, neighbor)
//
//                        finalAcc
//                    },
//                    (acc1, acc2) => {
//                        var finalAcc = acc1
//                        for(neighbor <- acc2){
//                            if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
//                                finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
//                            }
//                        }
//
//                        finalAcc
//                    }
//                )
//
//            val resulting = coreKNNs
//
//            cells.unpersist()
//            //        repartitioned.unpersist()
//            resulting.cache()
//
//            val incompleteCoreKNNs = resulting.filter(instance => {
//                val (_, kNeighbors) = instance
//                kNeighbors.contains(null)
//            }).cache()
//
//            if(incompleteCoreKNNs.count() == 0)
//                return resulting.map(tuple => (tuple._1.id, tuple._2))
//
//            val completeCoreKNNs = resulting.filter(instance => {
//                val (_, kNeighbors) = instance
//                !kNeighbors.contains(null)
//            }).map(point => (point._1.id, point._2))
//
//            val reverseNeighborsOfSaidInstanceByPivot = completeCoreKNNs
//                .filter{case (id, neighbors) => neighbors.map(_.id).contains(21564)}
//                .count
//
//            println(s"Said instance has ${reverseNeighborsOfSaidInstanceByPivot} reverse neighbors from its circunscription")
//
//            return
//
//            val incompleteCells = incompleteCoreKNNs.map(point => {
//                (point._1, point._2.filter(kNeighbor => kNeighbor != null).map(kNeighbor => kNeighbor.id))
//            })
//
//            val supportKNNs = incompleteCells.cartesian(instances)
//                .filter(pair => pair._1._1.id != pair._2.id && !pair._1._2.contains(pair._2.id))
//                .map(pair => (pair._1._1.id, new KNeighbor(pair._2.id, distanceFunction(pair._1._1.data, pair._2.data))))
//                .aggregateByKey(Array.fill[KNeighbor](k)(null))(
//                    (acc, neighbor) => {
//                        var finalAcc = acc
//                        if(acc.last == null || neighbor.distance < acc.last.distance)
//                            finalAcc = Utils.insertNeighborInArray(acc, neighbor)
//
//                        finalAcc
//                    },
//                    (acc1, acc2) => {
//                        var finalAcc = acc1
//                        for(neighbor <- acc2){
//                            if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
//                                finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
//                            }
//                        }
//
//                        finalAcc
//                    }
//                )
//
//            val incompleteCoreKNNsFixed = incompleteCoreKNNs.map(tuple => (tuple._1.id, tuple._2)).join(supportKNNs).map(tuple => {
//                val (instanceId, knns) = tuple
//                val (core, support) = knns
//                val filteredSupport = if(support.contains(null)) support.filter(n => n != null) else support
//                filteredSupport.foreach(supportNeighbor => {
//                    if(core.contains(null) || core.last.distance > supportNeighbor.distance){
//                        addNewNeighbor(core, supportNeighbor)
//                    }
//                })
//                (instanceId, core)
//            })
//
//            //        resulting.unpersist()
//            //        incompleteCoreKNNs.unpersist()
//            val kNeighbors = completeCoreKNNs.union(incompleteCoreKNNsFixed)
//            val insufficientPivots = cells.groupByKey().mapValues(iter => iter.toArray.length <= k)
//
//            val instancesAreInsufficient = cells.join(insufficientPivots).map{case (pivot, (point, isInsufficient)) => (point.id, isInsufficient)}
//            val kNeighborsWithInsufficient = kNeighbors.join(instancesAreInsufficient)
//                .filter{case (id, (neighbors, isInsufficient)) => neighbors.map(_.id).contains(21564)}
//                .cache
//
//            val insufficientReverseNeighborsOfSaidInstance = kNeighborsWithInsufficient.filter{case (id, (kNeighbors, isInsufficient)) => isInsufficient}
//            println(s"Said instance has ${kNeighborsWithInsufficient.count()} reverse neighbors of which ${insufficientReverseNeighborsOfSaidInstance.count()} are insufficient")
//
//
//
//
//
//
//
//
////            val approxRNeighbors = NeighborsReverser.findReverseNeighbors(approxKNeighbors)
////                .filter{case (id, _) => outliersIds.contains(id)}
////                .collect()
////                .find(t => t._1 == 21564)
//
//
////            val saveFile = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\approx-reverse-count.csv"
////            ReaderWriter.writeToFile(saveFile, s"id,reverse_count_from_approx\n${approxRNeighbors.mkString("\n")}")
//
//            println(s"---------------Done executing -------------------")
//        }
//        catch{
//            case e: Exception => {
//                println("-------------The execution didn't finish due to------------------")
//                println(e)
//            }
//        }
//    }

    def smallDataExperiment(args: Array[String]): Unit = {

        // seeds = 87654 458212 57124 6745 12541 7634 431 90783 34634 56342
        val pivotsAmount = 142
        val distanceFunction = euclidean
        val k = if (args.length > 0) args(10).toInt else 200
        val seed = if (args.length > 1) args(1).toInt else 87654
        val detectionMethod = if (args.length > 2) args(2) else "antihub"

        try {
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val detectionCriteria: detection.DetectionStrategy = detectionMethod match {
                case "antihub" => new detection.Antihub()
                case "ranked" => new detection.RankedReverseCount(k, 0.7)
                case "refined" => new detection.AntihubRefined(0.1, 0.3)
            }

            //            val rawData = ReaderWriter.readCSV(datasetPath, hasHeader = false)
            //            val instancesAndClassification = rawData.zipWithIndex.map { case (line, index) =>
            //                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            //                val classification = if (line.last == "1") "1.0" else "0.0"
            //                (Instance(index, attributes), classification)
            //            }.toSeq
            //
            //            val instances = instancesAndClassification.map(_._1)
            //            val classifications = instancesAndClassification.map { case (instance, classification) => (instance.id, classification) }

            val config = new SparkConf()
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.setMaster("local[*]")

            val spark = SparkSession.builder()
                .config(config)
                .appName(s"Small Data Test k: $k seed: $seed method: $detectionMethod")
                .getOrCreate();

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.collect().toSeq

            spark.stop()

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            val onStart = System.nanoTime()
            val outlierDegrees = new SmallDataDetector().detect(instances, k, pivotsAmount, seed, detectionCriteria, distanceFunction)
            val onFinish = System.nanoTime
            val duration = (onFinish - onStart) / 1000000

            config.setAppName("Any name")
            val sc = new SparkContext(config)

            val predictionsAndLabels = sc.parallelize(classifications).join(sc.parallelize(outlierDegrees)).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$k,$seed,$detectionMethod,${detectionMetrics.areaUnderROC()},$duration"
            saveStatistics(line)

            println(s"---------------Done executing-------------------")
        }
        catch {
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def studyOutliersFromBothSearches(): Unit = {
        val approximateFilename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results.csv"
        val exactFilename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results-fixed.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))
        val k = 800

        try{
            val approximateRawKNeighbors = sc.textFile(approximateFilename).map(line => line.split(","))

            val approximateKNeighbors = approximateRawKNeighbors.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val fullPath = System.getProperty("user.dir")
            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawInstances = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawInstances.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val idAndClassification = instancesAndClassification.map {case (instance, classification) => (instance.id, classification)}.cache

            val approximateDetectionResult = getDetectionResultFromKNeighbors(approximateKNeighbors).sortBy(_._2, ascending = false).zipWithIndex().map{ case ((id, degree), ranking) => (id, (degree, ranking))}
            val approximateResultWithClassification = idAndClassification.join(approximateDetectionResult).cache()
            val approximateOutliersResultWithClassification = approximateResultWithClassification.filter {case (id, (classification, degree)) => classification == "1.0"}

            val approximateRNeighbors = NeighborsReverser.findReverseNeighbors(approximateKNeighbors)
            val approximateOutliersResultWithClassificationAndRNeighbors = approximateOutliersResultWithClassification.join(approximateRNeighbors)

            val exactRawKNeighbors = sc.textFile(exactFilename).map(line => line.split(","))

            val exactKNeighbors = exactRawKNeighbors.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val exactRNeighbors = NeighborsReverser.findReverseNeighbors(exactKNeighbors)

            val exactDetectionResult = getDetectionResultFromKNeighbors(exactKNeighbors).sortBy(_._2, ascending = false).zipWithIndex().map{ case ((id, degree), ranking) => (id, (degree, ranking))}
            val exactResultWithClassification = idAndClassification.join(exactDetectionResult).cache()
            val exactOutliersResultWithClassification = exactResultWithClassification.filter {case (id, (classification, degree)) => classification == "1.0"}
            val exactOutliersResultWithClassificationAndRNeighbors = exactOutliersResultWithClassification.join(exactRNeighbors)


            // How many increased rank, how many decreased rknn, how many increased rank
            // How different are their kNeighbors
            /*
             How to calculate this? How many ids are present in both arrays from the total
            */
            val exactOutlierKNeighbors = exactKNeighbors.join(idAndClassification).filter {case (id, (neighbors, classification)) => classification == "1.0"}
            val approximateOutlierKNeighbors = approximateKNeighbors.join(idAndClassification).filter {case (id, (neighbors, classification)) => classification == "1.0"}

            val kNeighborsDifference = exactOutlierKNeighbors.join(approximateOutlierKNeighbors)
                .map {case (id, ((exactKN, c1), (approxKN, c2))) => {
                    val exactIds = exactKN.map(_.id).toSet
                    val approxIds = approxKN.map(_.id).toSet
                    val sharedAmount = exactIds.intersect(approxIds).size
                    (id, sharedAmount.toDouble/k.toDouble)
                }}


            val lines = approximateOutliersResultWithClassificationAndRNeighbors.join(exactOutliersResultWithClassificationAndRNeighbors)
                .join(kNeighborsDifference)
                .sortBy(_._2._1._1._1._2._1, ascending = false)
                .map{case (id, ((((approxClass, (approxDeg, approxRank)), approxRNeigh), ((exactClass, (exactDeg, exactRank)), exactRNeigh)), fraction)) => {
                    s"$id,${exactRNeigh.length},${approxRNeigh.length},${exactDeg},${approxDeg},${exactRank},${approxRank},${fraction}"
                }}

            val saveFile = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\results-comparison-arrowless.csv"
            ReaderWriter.writeToFile(saveFile, lines.collect().mkString("\n"))

            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def studyExactSearchResults(): Unit = {
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800, 1000).reverse
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results-fixed.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

        try{
            val rawKNeighbors = sc.textFile(filename).map(line => line.split(","))


            val kNeighbors = rawKNeighbors.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val fullPath = System.getProperty("user.dir")
            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawInstances = sc.textFile(datasetPath).map(line => line.split(","))
            val idAndClassification = rawInstances.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val classification = if(line.last == "1") "1.0" else "0.0"
                (index.toInt, classification)
            })

            for(k <- kValues){
                val slicedNeighbors = kNeighbors.mapValues(arr => arr.slice(0, k))

                val onReverse = System.nanoTime
                val rNeighbors = NeighborsReverser.findReverseNeighbors(slicedNeighbors).cache()
                rNeighbors.count()
                val onFinishReverse = System.nanoTime
                val reverseDuration = (onFinishReverse - onReverse) / 1000000

                val onDetection = System.nanoTime
                val antihub = new Antihub().antihub(rNeighbors).cache()
                antihub.count()
                val onFinishAntihub = System.nanoTime
                val ranked = new RankedReverseCount(0.7, k).calculateAnomalyDegree(rNeighbors, k).cache()
                ranked.count()
                val onFinishRanked = System.nanoTime
                val refined = new AntihubRefined(0.2, 0.3).antihubRefined(rNeighbors, antihub).cache()
                refined.count()
                val onFinishRefined = System.nanoTime

                val antihubDuration = (onFinishAntihub - onDetection) / 1000000
                val rankedDuration = (onFinishRanked - onFinishAntihub) / 1000000
                val refinedDuration = ((onFinishRefined - onFinishRanked) / 1000000) + antihubDuration

                val classifications = idAndClassification.map(tuple => (tuple._1, tuple._2)).cache()
                val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
                val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)

                val predictionsAndLabelsRanked = classifications.join(ranked).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
                val detectionMetricsRanked = new BinaryClassificationMetrics(predictionsAndLabelsRanked)

                val predictionsAndLabelsRefined = classifications.join(refined).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
                val detectionMetricsRefined = new BinaryClassificationMetrics(predictionsAndLabelsRefined)

                val searchDuration = -1
                val datasetSize = 50000
                val pivotsAmount = -1
                val method = "exhaustive"
                val seed = -1

                val antihubLine = s"${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,antihub,${detectionMetricsAntihub.areaUnderROC()},${detectionMetricsAntihub.areaUnderPR()},$searchDuration,$reverseDuration,$antihubDuration,${searchDuration + reverseDuration + antihubDuration}"
                saveStatistics(antihubLine)
                val rankedLine = s"${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,ranked,${detectionMetricsRanked.areaUnderROC()},${detectionMetricsRanked.areaUnderPR()},$searchDuration,$reverseDuration,$rankedDuration,${searchDuration + reverseDuration + rankedDuration}"
                saveStatistics(rankedLine)
                val refinedLine = s"${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,refined,${detectionMetricsRefined.areaUnderROC()},${detectionMetricsRefined.areaUnderPR()},$searchDuration,$reverseDuration,$refinedDuration,${searchDuration + reverseDuration + refinedDuration}"
                saveStatistics(refinedLine)
            }

//            val detectionResult = new Antihub().antihub(rNeighbors)
//            val resultWithClassification = idAndClassification.join(detectionResult).cache()
//            val sortedResults = resultWithClassification.sortBy(_._2._2, ascending=false).take(492)

//            if(sortedResults.count(t => t._2._1 == "1.0") != 492)
//                throw new Exception("There was a different amount of resultsWithClassification")

//            val truePositives = sortedResults.count { case (id, (classification, degree)) => classification == "1.0" }
//            val falsePositives = sortedResults.count {case (id, (classification, degree)) => classification == "0.0"}
//            val sortedResultsAreSorted = sortedResults.map(_._2._2).zipWithIndex.forall{case (deg, index) => {
//                if(index > 0){
//                    sortedResults(index - 1)._2._2 >= deg
//                }
//                else{
//                    true
//                }
//            }}

//            println(s"---------------Done executing -------------------\nIt was sorted: $sortedResultsAreSorted\nExact Search detects ${truePositives} true positives and ${falsePositives} false positives in the top most ${sortedResults.length} points")
            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def studyPivotsSearchResults(): Unit ={

        val approximateFilename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results.csv"
        val exactFilename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results-fixed.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

        try{
            val approximateRawKNeighbors = sc.textFile(approximateFilename).map(line => line.split(","))

            val approximateKNeighbors = approximateRawKNeighbors.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val fullPath = System.getProperty("user.dir")
            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawInstances = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawInstances.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1).cache
            val idAndClassification = instancesAndClassification.map {case (instance, classification) => (instance.id, classification)}.cache

            val pivotsIds = Array(6903,3878,7022,34506,35777,15998,47636,49858,35560,47169,30385,32323)
            val pivots = instances.filter(i => pivotsIds.contains(i.id))

            val approximateDetectionResult = getDetectionResultFromKNeighbors(approximateKNeighbors)

            val cells = instances.cartesian(pivots)
                .map(tuple => {
                    val (instance, pivot) = tuple
                    (instance, (pivot, euclidean(instance.data, pivot.data)))
                })
                .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
                .map(t => (t._2._1, t._1))

            val countPerPivot = cells.map {case (pivot, point) => (pivot, 1)}.reduceByKey(_+_)

            val pointsAndSearchCondition = cells.join(countPerPivot).map { case (pivot, (point, countInCell)) => (point, if(countInCell <= 800) "exact" else "cell-based")}

            // How many needed exact search
            val pointsNeedingExactSearch = pointsAndSearchCondition.filter{ case (point, searchCondition) => searchCondition == "exact"}.map{ case (point, searchCondition) => (point.id, searchCondition)}

            // How many needing exact search were outliers
            val needyPointsAndClassification = pointsNeedingExactSearch.join(idAndClassification)
            val needyOutliers = needyPointsAndClassification.filter {case (id, (searchType, classification)) => classification == "1.0"}

            // How many from the top 492 points needed exact search
            val sortedApproximateResults = approximateDetectionResult.sortBy(_._2, ascending=false).take(492)
            val sortedApproximateResultsRdd = sc.parallelize(sortedApproximateResults)
            val approximateTopDetectedWithClassification = idAndClassification.join(sortedApproximateResultsRdd).cache()
            val approximateDetectedOutliers = approximateTopDetectedWithClassification.filter {case (id, (classification, degree)) => classification == "1.0"}.map(t => (t._1, 1))
            val needyPointsOnTop = pointsNeedingExactSearch.join(sortedApproximateResultsRdd)

            // How accurate were the kNeighbors of the 492 top points
            val exactRawKNeighbors = sc.textFile(exactFilename).map(line => line.split(","))

            val exactKNeighbors = exactRawKNeighbors.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val exactDetectionResult = getDetectionResultFromKNeighbors(exactKNeighbors)
            val sortedExactResults = exactDetectionResult.sortBy(_._2, ascending=false).take(492)
            val sortedExactResultsRdd = sc.parallelize(sortedExactResults)
            val exactTopDetectedWithClassification = idAndClassification.join(sortedExactResultsRdd).cache()
            val exactDetectedOutliers = exactTopDetectedWithClassification.filter {case (id, (classification, degree)) => classification == "1.0"}.map(t => (t._1, 1))

            val outliersDetectedByBothJoin = exactDetectedOutliers.join(approximateDetectedOutliers)
            val outliersDetectedByBothIntersection = exactDetectedOutliers.intersection(approximateDetectedOutliers)

            println(s"---------------Done executing -------------------\nThe amount of points that needed exact search were ${pointsNeedingExactSearch.count()}.\nOf those, ${needyOutliers.count()} were outliers.\nThe needy points are ${needyPointsOnTop.count()} of the top 492.\nThe amount of outliers detected by both on the top 492 was ${outliersDetectedByBothJoin.count()} / ${outliersDetectedByBothIntersection.count()}. Exact detected outliers were ${exactDetectedOutliers.count()} and approximate detected outliers were ${approximateDetectedOutliers.count()}")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def getDetectionResultFromKNeighbors(kNeighbors: RDD[(Int, Array[KNeighbor])]): RDD[(Int, Double)] = {
        val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors)
        val detectionResult = new Antihub().antihub(rNeighbors)
        detectionResult
    }

    def checkOnMaxRefinedSearchResults(): Unit = {
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))


        try{
            val rawData = sc.textFile(filename).map(line => line.split(","))
            val idsAndLengths = rawData.map(arr => (arr(0), arr.length)).cache()
            val ids = idsAndLengths.map(_._1)
            val lengths = idsAndLengths.map(_._2)
            println(s"Amount of distinct ids: ${ids.distinct().count()}")
            println(s"All lines have same length: ${lengths.distinct().count() == 1}")

            val kNeighbors = rawData.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors)
            val detectionResult = new Antihub().antihub(rNeighbors)

            val fullPath = System.getProperty("user.dir")
            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawInstances = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawInstances.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()

            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
            val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
            println(s"Roc value is: ${detectionMetrics.areaUnderROC()}")

            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def saveSearchResultForMaxRefined(): Unit = {
        val k = 800
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            //            val datasetRelativePath = s"datasets\\iris-synthetic-2-to-double.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1).persist

            val pivotsIds = Array(6903,3878,7022,34506,35777,15998,47636,49858,35560,47169,30385,32323)
            val pivots = instances.filter(i => pivotsIds.contains(i.id))

            if(pivots.count() != 12)
                throw new Exception("Weird amount of pivots")

            // Create cells
            val cells = instances.cartesian(pivots)
                .map(tuple => {
                    val (instance, pivot) = tuple
                    (instance, (pivot, euclidean(instance.data, pivot.data)))
                })
                .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
                .map(t => (t._2._1, t._1))

            val coreKNNs = cells.join(cells)
                .filter{case (pivot, (ins1, ins2)) => ins1.id != ins2.id}
                .map(tuple => (tuple._2._1, new KNeighbor(tuple._2._2.id, BigDecimal(euclidean(tuple._2._1.data, tuple._2._2.data)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)))
                .aggregateByKey(Array.fill[KNeighbor](k)(null))(
                    (acc, neighbor) => {
                        var finalAcc = acc
                        if(acc.last == null || neighbor.distance < acc.last.distance)
                            finalAcc = Utils.insertNeighborInArray(acc, neighbor)

                        finalAcc
                    },
                    (acc1, acc2) => {
                        var finalAcc = acc1
                        for(neighbor <- acc2){
                            if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                                finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                            }
                        }

                        finalAcc
                    }
                )
            val resulting = coreKNNs

            resulting.cache()

            val incompleteCoreKNNs = resulting.filter(instance => {
                val (_, kNeighbors) = instance
                kNeighbors.contains(null)
            })

            incompleteCoreKNNs.cache()

//            if(incompleteCoreKNNs.count() == 0)
//                return resulting.map(tuple => (tuple._1.id, tuple._2))

            val completeCoreKNNs = resulting.filter(instance => {
                val (_, kNeighbors) = instance
                !kNeighbors.contains(null)
            }).map(point => (point._1.id, point._2))

            val incompleteCells = incompleteCoreKNNs.map(point => {
                (point._1, point._2.filter(kNeighbor => kNeighbor != null).map(kNeighbor => kNeighbor.id))
            })

            val supportKNNs = incompleteCells.cartesian(instances)
                .filter(pair => pair._1._1.id != pair._2.id && !pair._1._2.contains(pair._2.id))
                .map(pair => (pair._1._1.id, new KNeighbor(pair._2.id, BigDecimal(euclidean(pair._1._1.data, pair._2.data)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)))
                .aggregateByKey(Array.fill[KNeighbor](k)(null))(
                    (acc, neighbor) => {
                        var finalAcc = acc
                        if(acc.last == null || neighbor.distance < acc.last.distance)
                            finalAcc = Utils.insertNeighborInArray(acc, neighbor)

                        finalAcc
                    },
                    (acc1, acc2) => {
                        var finalAcc = acc1
                        for(neighbor <- acc2){
                            if(neighbor != null && (finalAcc.last == null || neighbor.distance < finalAcc.last.distance)){
                                finalAcc = Utils.insertNeighborInArray(finalAcc, neighbor)
                            }
                        }

                        finalAcc
                    }
                )

            val incompleteCoreKNNsFixed = incompleteCoreKNNs.map(tuple => (tuple._1.id, tuple._2)).join(supportKNNs).map(tuple => {
                val (instanceId, knns) = tuple
                val (core, support) = knns
                val filteredSupport = if(support.contains(null)) support.filter(n => n != null) else support
                filteredSupport.foreach(supportNeighbor => {
                    if(core.contains(null) || core.last.distance > supportNeighbor.distance){
                        addNewNeighbor(core, supportNeighbor)
                    }
                })
                (instanceId, core)
            })

            val kNeighbors = completeCoreKNNs.union(incompleteCoreKNNsFixed).filter{case (id, neighbors) => id.toInt > 40000 && id.toInt <= 50000}.collect()

            val lines = kNeighbors.map{case (id, kn) =>
                s"$id,${kn.map(n => s"${n.id}::${n.distance}").mkString(",")}"
            }.mkString("\n")

            val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results.csv"

            val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
            val updatedRecords = s"${previousRecordsText}\n$lines"
            ReaderWriter.writeToFile(filename, updatedRecords)

            println(s"---------------Done executing -------------------")
            //            if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0){
            //                throw new Exception("There are element with null neighbors")
            //            }
            //
            //            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors).cache()
            //
            //            val detectionResult = new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors).cache
            //
            //            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}
            //            val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            //            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
            //
            //            val resultsFileId = s"${if(datasetSize == -1) "full" else s"${datasetSize}"}_${k}_exhaustive_refined"
            //            saveStatistics(resultsFileId, detectionMetrics.areaUnderROC(), detectionMetrics.areaUnderPR(), "")

            // Save results for further use
            //            val lines = rNeighbors.join(detectionResult).join(kNeighbors).map{case (id, ((rn, degree), kn)) =>
            //                s"$id,${rn.length},$degree,${kn.map(n => s"${n.id}-${n.distance}").mkString(",")}"
            //            }
            //
            //            val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results.csv"
            //            ReaderWriter.writeToFile(filename, lines.collect().mkString("\n"))
            //
            //            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)

            }
        }
    }

    def savePivotsCombos(): Unit = {

        val k = 2000
        val pivotsAmount = 12
        val detectionMethods = Array("antihub", "ranked", "refined")
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)


            val pivots = instances.takeSample(withReplacement = false, pivotsAmount) // seed=87654
//            val pivotsIds = Array("6903","3878","7022","34506","35777","15998","47636","49858","35560","47169","30385","32323")
//            val pivots = instances.filter(i => pivotsIds.contains(i.id)).collect()

            val kNeighbors = new GroupedByPivot(pivots).findApproximateKNeighbors(instances, k, euclidean, sc).cache()

            if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0){
                throw new Exception("There are element with null neighbors")
            }

            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors).cache()

            detectionMethods.foreach(detectionMethod => {

                val detectionResult = (detectionMethod match {
                    case "antihub" => new Antihub().antihub(rNeighbors)
                    case "ranked" => new RankedReverseCount(0.7, k).calculateAnomalyDegree(rNeighbors, k)
//                    case "refined" => new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors)
                }).cache

                val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
                val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
                val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

                val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pivots-results.csv"
                val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
                val updatedRecords = s"${previousRecordsText}\n\n$detectionMethod,${detectionMetrics.areaUnderROC()},${pivots.map(_.id).mkString(",")}"
                ReaderWriter.writeToFile(filename, updatedRecords)

                println(s"-----------------------------------\nArea under ROC: ${detectionMetrics.areaUnderROC()}\nArea under Precision-Recall Curve: ${detectionMetrics.areaUnderPR()}\n-----------------------------------")

            })

            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }



    def fixSavedResultsSeparatorIssue(): Unit = {
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results-fiasco.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

        val rawData = sc.textFile(filename).map(line => line.split(","))
        val fixed = rawData.map(parts => {
            parts.map(str => {
                if(str.contains("-")){
                    val tokens = str.split("-")
                    if(tokens.length == 2){
                        s"${tokens(0)}::${tokens(1)}"
                    }
                    else{
                        s"${tokens(0)}::${tokens(1)}-${tokens(2)}"
                    }
                }
                else{
                    str
                }
            })
        })

        val filenameSave = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\pknn-max-refined-results-fixed.csv"

        ReaderWriter.writeToFile(filenameSave, fixed.collect().map(parts => parts.mkString(",")).mkString("\n"))
    }

    def detectionFromExactSaveResults(): Unit = {
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results-fixed.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

        try{

            val rawData = sc.textFile(filename).map(line => line.split(","))

            val kNeighbors = rawData.map(line => {
                val id = line(0).toInt
                val neighbors = line.slice(1, line.length).map(token => {
                    val tokenParts = token.split("::")
                    if(tokenParts.length < 2){
                        throw new Exception(s"For id ${id} there is a tokenParts with less than two elements: ${tokenParts.mkString(",")}")
                    }
                    new KNeighbor(tokenParts(0).toInt, tokenParts(1).toDouble)
                })

                (id, neighbors)
            })

            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors)
            val detectionResult = new Antihub().antihub(rNeighbors)

            val fullPath = System.getProperty("user.dir")
            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled_50000.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val rawInstances = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawInstances.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()

            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
            val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
            println(s"Roc value is: ${detectionMetrics.areaUnderROC()}")

            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def checkOnExactSaveResults(): Unit = {
        val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results.csv"
        val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

        val rawData = sc.textFile(filename).map(line => line.split(","))
        val idsAndLengths = rawData.map(arr => (arr(0), arr.length)).cache()
        val ids = idsAndLengths.map(_._1)
        val lengths = idsAndLengths.map(_._2)
        println(s"Amount of distinct ids: ${ids.distinct().count()}")
        println(s"All lines have same length: ${lengths.distinct().count() == 1}")
    }

    def exactSearchToSaveResults(): Unit ={
        val k = 800
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
//            val datasetRelativePath = s"datasets\\iris-synthetic-2-to-double.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if(line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1).persist

            val luckyOnes = instances.filter(instance => instance.id.toInt > 40000 && instance.id.toInt <= 50000).repartition(16).persist

            val fullyMappedInstances = luckyOnes.cartesian(instances)
                .filter(instances_tuple => instances_tuple._1.id != instances_tuple._2.id)
                .map(instances_tuple => {
                    val (ins1, ins2) = instances_tuple
                    (
                        ins1.id,
                        new KNeighbor(
                            ins2.id,
                            BigDecimal(euclidean(ins1.data, ins2.data)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
                        )
                    )
                })

            val x = fullyMappedInstances.aggregateByKey(Array.fill[KNeighbor](k)(null))(
                (acc, neighbor) => {
                    var finalAcc = acc.map(n => if(n != null) n.copy(n.id, n.distance) else null)
                    if(acc.last == null || neighbor.distance < acc.last.distance)
                        Utils.mergeNeighborIntoArray(finalAcc, neighbor)
                    else{
                        finalAcc
                    }
                },
                (acc1, acc2) => {
                    Utils.mergeTwoNeighborArrays(acc1, acc2)
                }
            )

//            val kNeighbors = new ExhaustiveBigData().findKNeighbors(instances, k, euclidean, sc).collect()
            val kNeighbors = x.collect()

            val lines = kNeighbors.map{case (id, kn) =>
                s"$id,${kn.map(n => s"${n.id}-${n.distance}").mkString(",")}"
            }.mkString("\n")

            val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results.csv"

            val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
            val updatedRecords = s"${previousRecordsText}\n$lines"
            ReaderWriter.writeToFile(filename, updatedRecords)

            println(s"---------------Done executing -------------------")
//            if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0){
//                throw new Exception("There are element with null neighbors")
//            }
//
//            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors).cache()
//
//            val detectionResult = new AntihubRefined(new AntihubRefinedParams(0.2, 0.3)).antihubRefined(rNeighbors).cache
//
//            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}
//            val predictionsAndLabels = classifications.join(detectionResult).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
//            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)
//
//            val resultsFileId = s"${if(datasetSize == -1) "full" else s"${datasetSize}"}_${k}_exhaustive_refined"
//            saveStatistics(resultsFileId, detectionMetrics.areaUnderROC(), detectionMetrics.areaUnderPR(), "")

            // Save results for further use
//            val lines = rNeighbors.join(detectionResult).join(kNeighbors).map{case (id, ((rn, degree), kn)) =>
//                s"$id,${rn.length},$degree,${kn.map(n => s"${n.id}-${n.distance}").mkString(",")}"
//            }
//
//            val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\exact-results.csv"
//            ReaderWriter.writeToFile(filename, lines.collect().mkString("\n"))
//
//            println(s"---------------Done executing -------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def comparingSearchMethods(args: Array[String]): Unit = {

        val pivotsAmount = 25
        val k = args(1).toInt
        val method = args(2)
        val seed = args(3).toInt
        val datasetSize = 50000

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val onStart = System.nanoTime

            val sc = new SparkContext(new SparkConf().setAppName("Scaled creditcard test"))

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1)

            val pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed=seed)
            val kNeighbors =  (if(method == "classic"){
                new GroupedByPivot(pivots).findApproximateKNeighbors(instances, k, euclidean, sc).cache()
            }
            else if(method == "broadcasted"){
                new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc).cache()
            }
            else if(method == "shorty"){
                new GroupedByPivot(pivots).findApproximateKNeighborsShorty(instances, k, euclidean, sc).cache()
            }
            else{
                throw new Exception("There is no search method")
            }).cache()

            kNeighbors.count()
            val onFinishSearch = System.nanoTime
            val searchDuration = (onFinishSearch - onStart) / 1000000

            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors)
            val antihub = new Antihub().antihub(rNeighbors)

            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2))
            val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)

            val line = s"$k,$pivotsAmount,$seed,$method,$searchDuration,${detectionMetricsAntihub.areaUnderROC()}"
            saveStatistics(line, file=s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\search-results.csv")

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def mainExperiment(args: Array[String]): Unit = {

        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 142
        val k = if(args.length > 2) args(2).toInt else 1
        val seed = if(args.length > 3) args(3).toInt else 12541
        val datasetSize = if(args.length > 4) args(4).toInt else -1
        val method = if(args.length > 5) args(5) else "antihub"

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

//            val onStart = System.nanoTime

            val config = new SparkConf().setAppName("Scaled creditcard test")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))
//            config.set("spark.executor.memory", "12g")
//            config.set("spark.driver.maxResultSize", "1g")
//            config.set("spark.default.parallelism", "48")

            val spark = SparkSession.builder()
                .config(config)
                .appName("Scaled creditcard test")
                .getOrCreate();

            val sc = spark.sparkContext

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.cache()

            val instances = instancesAndClassification.map(_._1).cache()
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

//            val pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed=seed)
//            val kNeighbors = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, spark.sparkContext).cache()
//
//            if(kNeighbors.filter(tuple => tuple._2.contains(null)).count() > 0) {
//                throw new Exception("There are elements with null neighbors")
//            }
//
//            val onFinishSearch = System.nanoTime
//            val searchDuration = (onFinishSearch - onStart) / 1000000
//
//            val onReverse = System.nanoTime
//            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors).cache()
//            rNeighbors.count()
//            val onFinishReverse = System.nanoTime
//            val reverseDuration = (onFinishReverse - onReverse) / 1000000
//            kNeighbors.unpersist()
//
//            val x = (0 until 284000).to(HashSet)
//            val set = new HashSet[Int]()
//
//            val onDetection = System.nanoTime
//            val antihub = new Antihub().antihub(rNeighbors).cache()
//            antihub.count()
//            val onFinishAntihub = System.nanoTime
//            val ranked = new RankedReverseCount(0.7, k).calculateAnomalyDegree(rNeighbors, k).cache()
//            ranked.count()
//            val onFinishRanked = System.nanoTime
//            val refined = new AntihubRefined(0.2, 0.3).antihubRefined(rNeighbors, antihub).cache()
//            refined.count()
//            val onFinishRefined = System.nanoTime
//
//            val antihubDuration = (onFinishAntihub - onDetection) / 1000000
//            val rankedDuration = (onFinishRanked - onFinishAntihub) / 1000000
//            val refinedDuration = ((onFinishRefined - onFinishRanked) / 1000000) + antihubDuration
//
//            val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
//            val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)
//
//            val predictionsAndLabelsRanked = classifications.join(ranked).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
//            val detectionMetricsRanked = new BinaryClassificationMetrics(predictionsAndLabelsRanked)
//
//            val predictionsAndLabelsRefined = classifications.join(refined).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
//            val detectionMetricsRefined = new BinaryClassificationMetrics(predictionsAndLabelsRefined)
//
////            val elapsedTimeAntihub = s"s: ${searchDuration} - r: ${reverseDuration} - d: ${antihubDuration} = ${searchDuration + reverseDuration + antihubDuration}ms"
////            val elapsedTimeRanked = s"s: ${searchDuration} - r: ${reverseDuration} - d: ${rankedDuration} = ${searchDuration + reverseDuration + rankedDuration}ms"
////            val elapsedTimeRefined = s"s: ${searchDuration} - r: ${reverseDuration} - d: ${refinedDuration} = ${searchDuration + reverseDuration + refinedDuration}ms"
//
//            val antihubLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,antihub,${detectionMetricsAntihub.areaUnderROC()},${detectionMetricsAntihub.areaUnderPR()},$searchDuration,$reverseDuration,$antihubDuration,${searchDuration + reverseDuration + antihubDuration}"
//            saveStatistics(antihubLine)
//            val rankedLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,ranked,${detectionMetricsRanked.areaUnderROC()},${detectionMetricsRanked.areaUnderPR()},$searchDuration,$reverseDuration,$rankedDuration,${searchDuration + reverseDuration + rankedDuration}"
//            saveStatistics(rankedLine)
//            val refinedLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$method,$seed,refined,${detectionMetricsRefined.areaUnderROC()},${detectionMetricsRefined.areaUnderPR()},$searchDuration,$reverseDuration,$refinedDuration,${searchDuration + reverseDuration + refinedDuration}"
//            saveStatistics(refinedLine)

            val onStart = System.nanoTime()
            val antihub = Antihub.detectRepartitionedLightweightJoin(instances, pivotsAmount, seed, k, euclidean, sc).persist
            antihub.count()
            val onFinish = System.nanoTime()
            val duration = (onFinish - onStart) / 1000000
            val predictionsAndLabels = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val roc = new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC()
            antihub.unpersist()

            val line = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$seed,$method,$roc,$duration"
            saveStatistics(line, s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\antihub_full_implementations.csv")

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
//                System.in.read()
            }
        }
    }

    def byPartitionExperiment(args: Array[String]) = {
        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 142
        val k = if(args.length > 2) args(2).toInt else 10
        val seed = if(args.length > 3) args(3).toInt else 12541
        val datasetSize = if(args.length > 4) args(4).toInt else -1
        val distanceFunction = euclidean

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf().setAppName("Scaled creditcard test")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))

            val spark = SparkSession.builder()
                .config(config)
                .appName("Scaled creditcard test")
                .getOrCreate();

            val sc = spark.sparkContext

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.cache()

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            val onStart = System.nanoTime()

            val sample = instances.takeSample(withReplacement = false, pivotsAmount, seed)
            val pivots = sc.broadcast(sample)

            // Create cells
            val cells = instances.map(instance => {
                val closestPivot = pivots.value
                    .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                    .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

                (closestPivot._1, instance)
            }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

//            val pivotsWithCounts = sc.broadcast(cells.mapValues{_ => 1}.reduceByKey{_+_}.collect)
//
//            val pivotsToInstance = instances.flatMap(instance => {
//                val pivotsWithCountAndDist = pivotsWithCounts.value
//                    .map(pivot => (pivot._1, pivot._2, distanceFunction(pivot._1.data, instance.data)))
//
//                selectMinimumClosestPivotsRec(instance, k, pivotsWithCountAndDist)
//            }).partitionBy(new PivotsPartitioner(pivotsAmount, pivots.value.map(_.id))).persist()

            val rNeighbors = cells.mapPartitions(iter => {
                // All elements from the same partition should belong to the same pivot
                val elements = iter.toArray.map{case (pivot, instance) => instance}
                val allKNeighbors = elements.map(el => (el.id, new ExhaustiveSmallData().findQueryKNeighbors(el, elements,k, distanceFunction)))
                val filteredKNeighbors = allKNeighbors.map{case (id, neighbors) => (id, neighbors.filter(n => n != null))}
                val rNeighbors = filteredKNeighbors.flatMap{case (instanceId, kNeighbors) =>
                    kNeighbors.zipWithIndex.map{case (neighbor, index) => (neighbor.id, new RNeighbor(instanceId, index))}
                }.groupMap{case (instanceId, _) => instanceId}{case (_, rNeighbor) => rNeighbor}.toArray

                val elementsSet = mutable.HashSet.from(elements.map(_.id))
                rNeighbors.foreach(pair => elementsSet.remove(pair._1))

                val elementsWithoutRNeighbors = elementsSet.map(id => (id, Array.empty[RNeighbor])).toArray

                Iterator.from(rNeighbors.concat(elementsWithoutRNeighbors))
            }).persist()
            rNeighbors.count
            val onFinishSearch = System.nanoTime()
            val searchDuration = (onFinishSearch - onStart) / 1000000

            val onDetection = System.nanoTime
            val antihub = rNeighbors.mapPartitions(iter => Iterator.from(new detection.Antihub().scoreInstances(iter.toArray))).persist()
            antihub.count()
            val onFinishAntihub = System.nanoTime
            val antihubDuration = (onFinishAntihub - onDetection) / 1000000
            val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)


            val ranked = rNeighbors.mapPartitions(iter => Iterator.from(new detection.RankedReverseCount(k, 0.7).scoreInstances(iter.toArray))).persist()
            ranked.count()
            val onFinishRanked = System.nanoTime
            val rankedDuration = (onFinishRanked - onFinishAntihub) / 1000000
            val predictionsAndLabelsRanked = classifications.join(ranked).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsRanked = new BinaryClassificationMetrics(predictionsAndLabelsRanked)


            val refined = rNeighbors.mapPartitions(iter => Iterator.from(new detection.AntihubRefined(0.1, 0.3).scoreInstances(iter.toArray))).persist()
            refined.count()
            val onFinishRefined = System.nanoTime
            val refinedDuration = ((onFinishRefined - onFinishRanked) / 1000000) + antihubDuration
            val predictionsAndLabelsRefined = classifications.join(refined).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetricsRefined = new BinaryClassificationMetrics(predictionsAndLabelsRefined)


            val antihubLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$seed,antihub,${detectionMetricsAntihub.areaUnderROC()},${detectionMetricsAntihub.areaUnderPR()},$searchDuration,$antihubDuration,${searchDuration + antihubDuration}"
            saveStatistics(antihubLine)
            val rankedLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$seed,ranked,${detectionMetricsRanked.areaUnderROC()},${detectionMetricsRanked.areaUnderPR()},$searchDuration,$rankedDuration,${searchDuration + rankedDuration}"
            saveStatistics(rankedLine)
            val refinedLine = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$seed,refined,${detectionMetricsRefined.areaUnderROC()},${detectionMetricsRefined.areaUnderPR()},$searchDuration,$refinedDuration,${searchDuration + refinedDuration}"
            saveStatistics(refinedLine)

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                System.in.read()
            }
        }
    }

    class CustomSparkListener extends SparkListener {

        var totalProcessingTime: Long = 0
        var totalNetworkTime: Long = 0

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            val metrics = taskEnd.taskMetrics
            if (metrics != null) {
                val processingTime = metrics.executorRunTime
                val networkTime = metrics.shuffleReadMetrics.fetchWaitTime + metrics.shuffleWriteMetrics.writeTime / 1000000

                totalProcessingTime += processingTime
                totalNetworkTime += networkTime
            }
        }
    }

    def byPartitionFullLengthExperiment(args: Array[String]): Unit = {
        val datetime = java.time.LocalDateTime.now()
        val dateString = s"${datetime.getYear}-${datetime.getMonthValue}-${datetime.getDayOfMonth}_${datetime.getHour}:${datetime.getMinute}"
        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 95
        val k = if(args.length > 2) args(2).toInt else 600
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
            config.set("spark.default.parallelism", "128")

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

//            val customListener = new CustomSparkListener
//            spark.sparkContext.addSparkListener(customListener)

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
            val pivotDuration = (onPivotEnd - onPivotStart) / 1000000

            val onStart = System.nanoTime()
            val outlierDegrees = new SameThingByPartition().detectAnomaliesNoOneLeftBehind(instances, pivotsAmount, seed, k, distanceFunction, sc, detectionCriteria, selectedPivots).persist()
            outlierDegrees.count()
            val onFinish = System.nanoTime
//            spark.sparkContext.removeSparkListener(customListener)
            val duration = (onFinish - onStart) / 1000000
            val predictionsAndLabels = classifications.join(outlierDegrees).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$nodes,${if(datasetSize == -1) "full" else s"$datasetSize"},$k,$pivotsAmount,$seed,$detectionMethod,${detectionMetrics.areaUnderROC()},${detectionMetrics.areaUnderPR()},0,0,$duration,$dateString,$pivotStrategy,${sc.defaultParallelism},$pivotDuration"
//            val line = s"$nodes,$k,$seed,${detectionMetrics.areaUnderROC()},${customListener.totalProcessingTime},${customListener.totalNetworkTime},$duration"
            saveStatistics(line)

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                System.in.read()
            }
        }
    }

    def antihubExperiment(): Unit = {

        val pivotsAmount = 142
        val k = 400
        val seed = 12541
        val datasetSize = -1

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf().setAppName("Scaled creditcard test")
            config.setMaster("local[*]")
            config.set("spark.executor.memory", "12g")
            config.set("spark.default.parallelism", "48")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))

            val sc = new SparkContext(config)

            val rawData = sc.textFile(datasetPath).map(line => line.split(","))
            val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
                val (line, index) = tuple
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (new Instance(index.toInt, attributes), classification)
            }).cache()
            val instances = instancesAndClassification.map(_._1).repartition(48).cache()
            val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2)).cache()

            val onStart3 = System.nanoTime

            val antihub3 = Antihub.detect(instances, pivotsAmount, seed, k, euclidean, sc).cache()
            antihub3.count()

            val onFinish3 = System.nanoTime
            val duration3 = (onFinish3 - onStart3) / 1000000

            val predictionsAndLabels3 = classifications.join(antihub3).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val roc3 = new BinaryClassificationMetrics(predictionsAndLabels3).areaUnderROC()

            val onStart1 = System.nanoTime

            val pivots = instances.takeSample(withReplacement = false, pivotsAmount, seed=seed)
            val kNeighbors = new GroupedByPivot(pivots).findApproximateKNeighborsWithBroadcastedPivots(instances, k, euclidean, sc).cache()
            val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors).cache()
            val antihub1 = new Antihub().antihub(rNeighbors).cache()
            antihub1.count()

            val onFinish1 = System.nanoTime
            val duration1 = (onFinish1 - onStart1) / 1000000

            val predictionsAndLabels1 = classifications.join(antihub1).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val roc1 = new BinaryClassificationMetrics(predictionsAndLabels1).areaUnderROC()

            val onStart2 = System.nanoTime

            val antihub2 = Antihub.detect(instances, pivotsAmount, seed, k, euclidean, sc).cache()
            antihub2.count()

            val onFinish2 = System.nanoTime
            val duration2 = (onFinish2 - onStart2) / 1000000

            val predictionsAndLabels2 = classifications.join(antihub2).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val roc2 = new BinaryClassificationMetrics(predictionsAndLabels2).areaUnderROC()

            println(s"---------------Done executing-------------------\nRegular antihub took: ${duration1}ms with roc: $roc1\nFull antihub with flag took: ${duration2}ms with roc: $roc2\nFull antihub without flag took: ${duration3}ms with roc: $roc3")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                System.in.wait()
            }
        }
    }

    def mainStructured(args: Array[String]): Unit ={

        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 25
        val k = if(args.length > 2) args(2).toInt else 800
        val seed = if(args.length > 3) args(3).toInt else 12541
        val datasetSize = if(args.length > 4) args(4).toInt else 50000
        val method = if(args.length > 5) args(5) else "broadcastedTailRec"

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf()
            config.setMaster("local[*]")
            config.set("spark.executor.memory", "12g")
            config.set("spark.default.parallelism", "48")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))

            val spark = SparkSession.builder()
                .config(config)
                .appName("Test Structured Antihub")
                .getOrCreate();

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.cache()
            val instances = spark.createDataset(instancesAndClassification.map(_._1).repartition(48)).cache()
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            val start = System.nanoTime()
            val antihub = StructuredAntihub.detectFlag(instances, pivotsAmount, seed, k, euclidean, spark).cache()
            antihub.count()
            val finish = System.nanoTime()
            val duration = (finish - start) / 1000000

            val startStruct = System.nanoTime()
            val antihubStruct = StructuredAntihub.detect(instances, pivotsAmount, seed, k, euclidean, spark).cache()
            antihubStruct.count
            val finishStruct = System.nanoTime()
            val durationStruct = (finishStruct - startStruct) / 1000000

            val instancesRDD = instances.rdd.cache()
            instancesRDD.count

            val startUns = System.nanoTime()
            val antihubUns = Antihub.detect(instancesRDD, pivotsAmount, seed, k, euclidean, spark.sparkContext).cache()
            antihubUns.count
            val finishUns = System.nanoTime()
            val durationUns = (finishUns - startUns) / 1000000

            val predictionsAndLabels = classifications.join(antihub.rdd).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val roc = new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC()

            val predictionsAndLabelsStruct = classifications.join(antihubStruct.rdd).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val rocStruct = new BinaryClassificationMetrics(predictionsAndLabelsStruct).areaUnderROC()

            val predictionsAndLabelsUns = classifications.join(antihubUns).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val rocUns = new BinaryClassificationMetrics(predictionsAndLabelsUns).areaUnderROC()

            println(s"---------------Done executing-------------------")
            println(s"---------------Structured Flag - Roc: ${roc}     Duration: ${duration}ms-------------------")
            println(s"---------------Structured - Roc: ${rocStruct}     Duration: ${durationStruct}ms-------------------")
            println(s"---------------Unstructured - Roc: ${rocUns}     Duration: ${durationUns}ms-------------------")
            System.in.read()
            spark.stop()
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
                System.in.read()
            }
        }
    }

    def testStructured_vs_Unstructured(args: Array[String]): Unit ={

        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 25
        val k = if(args.length > 2) args(2).toInt else 800
        val seed = if(args.length > 3) args(3).toInt else 12541
        val datasetSize = if(args.length > 4) args(4).toInt else 50000
        val method = if(args.length > 5) args(5) else "broadcastedTailRec"

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\creditcardMinMaxScaled${if(datasetSize == -1) "" else s"_${datasetSize}"}.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf()
//            config.setMaster("local[*]")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))

            val spark = SparkSession.builder()
                .config(config)
                .appName("Test Structured Antihub")
                .getOrCreate();

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.cache()
            val instances = spark.createDataset(instancesAndClassification.map(_._1)).cache()
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            val startStruct = System.nanoTime()
            val antihubStruct = StructuredAntihub.detect(instances, pivotsAmount, seed, k, euclidean, spark).cache()
            antihubStruct.count()
            val finishStruct = System.nanoTime()
            val durationStruct = (finishStruct - startStruct) / 1000000
            val predictionsAndLabelsStruct = classifications.join(antihubStruct.rdd).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val rocStruct = new BinaryClassificationMetrics(predictionsAndLabelsStruct).areaUnderROC()
            antihubStruct.unpersist()

            val startStructFlag = System.nanoTime()
            val antihubStructFlag = StructuredAntihub.detectFlag(instances, pivotsAmount, seed, k, euclidean, spark).cache()
            antihubStructFlag.count()
            val finishStructFlag = System.nanoTime()
            val durationStructFlag = (finishStructFlag - startStructFlag) / 1000000
            val predictionsAndLabelsStructFlag = classifications.join(antihubStructFlag.rdd).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val rocStructFlag = new BinaryClassificationMetrics(predictionsAndLabelsStructFlag).areaUnderROC()
            antihubStructFlag.unpersist()

            val instancesRDD = instances.rdd
            val startUnstruct = System.nanoTime()
            val antihubUnstruct = Antihub.detect(instancesRDD, pivotsAmount, seed, k, euclidean, spark.sparkContext).cache()
            antihubUnstruct.count()
            val finishUnstruct = System.nanoTime()
            val durationUnstruct = (finishUnstruct - startUnstruct) / 1000000
            val predictionsAndLabelsUnstruct = classifications.join(antihubUnstruct).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val rocUnstruct = new BinaryClassificationMetrics(predictionsAndLabelsUnstruct).areaUnderROC()
            antihubUnstruct.unpersist()

            val filename = s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\struct_vs_unstruct.csv"
            val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
            val updatedRecords = s"$previousRecordsText\n$nodes,$pivotsAmount,$k,$seed,$rocUnstruct,$durationUnstruct,$rocStruct,$durationStruct,$rocStructFlag,$durationStructFlag"
            ReaderWriter.writeToFile(filename, updatedRecords)
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def saveStatistics(line: String, file: String = ""): Unit = {
        val filename = if(file == "") s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\by_partition_experiments.csv" else file
        val previousRecordsText = ReaderWriter.readCSV(filename, hasHeader=false).map(line => line.mkString(",")).mkString("\n")
        val updatedRecords = s"$previousRecordsText\n$line"
        ReaderWriter.writeToFile(filename, updatedRecords)
    }

    def syntheticIrisExperiment(args: Array[String]): Unit = {
        val k = args(0).toInt

        val sc = new SparkContext(new SparkConf().setAppName("Synthetic Iris test"))
        val fullPath = System.getProperty("user.dir")
        val datasetFilename = "testingDatasets/iris-synthetic-2.csv"
        val datasetPath = s"${fullPath}\\${datasetFilename}"
        val distanceFunction: DistanceFunction = DistanceFunctions.euclidean

        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
        val instancesAndClassification = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            val classification = if(line.last == "Iris-setosa") "1.0" else "0.0"
            (new Instance(index.toInt, attributes), classification)
        }).cache()
        val instances = instancesAndClassification.map(_._1)

        val kNeighbors = ExhaustiveBigData.findKNeighbors(instances, k, distanceFunction, sc)

        val rNeighbors = NeighborsReverser.findReverseNeighbors(kNeighbors)

        val antihub = new Antihub().antihub(rNeighbors)
        val ranked = new RankedReverseCount(0.7, k).calculateAnomalyDegree(rNeighbors, k)
        val refined = new AntihubRefined(0.2, 0.3).antihubRefined(rNeighbors, antihub)

        val classifications = instancesAndClassification.map(tuple => (tuple._1.id, tuple._2)).cache()
        val predictionsAndLabelsAntihub = classifications.join(antihub).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
        val detectionMetricsAntihub = new BinaryClassificationMetrics(predictionsAndLabelsAntihub)

        val predictionsAndLabelsRanked = classifications.join(ranked).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
        val detectionMetricsRanked = new BinaryClassificationMetrics(predictionsAndLabelsRanked)

        val predictionsAndLabelsRefined = classifications.join(refined).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
        val detectionMetricsRefined = new BinaryClassificationMetrics(predictionsAndLabelsRefined)

        println(s"For k = $k:\nAntihub roc: ${detectionMetricsAntihub.areaUnderROC()}\nRanked roc: ${detectionMetricsRanked.areaUnderROC()}\nAntihub Refined roc: ${detectionMetricsRefined.areaUnderROC()}")
    }

    def loadNeighborsFromSavedSearch(
        savedResultsFilename: String,
        hasHeader: Boolean
    ): Array[(String, Array[KNeighbor])] = {
        val neighborsRawData = ReaderWriter.readCSV(savedResultsFilename, hasHeader=hasHeader)
        val neighbors = neighborsRawData.map(line => {
            // First element is instance id\
            val id = line(0)
            val kNeighbors = line.slice(1, line.length).map(token => {
                val neighborProperties = token.split(";")
                new KNeighbor(id=neighborProperties(0).toInt, distance=neighborProperties(1).toDouble)
            })

            (id, kNeighbors)
        })

        neighbors
    }
}
