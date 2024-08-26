package rknn_outlier_detection

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.Instance
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.search.pivot_based.{FarthestFirstTraversal, IncrementalSelection, PersistentRandom}
import rknn_outlier_detection.big_data.search.pivot_based.{BalancingPivotsOccurrences, PkNN}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object PivotSelectionExperiment {

    def main(args: Array[String]): Unit = {
        testPivotsInfluenceInExecution()
    }

    def testExecutionTimeForVaryingPivotsAmount(): Unit = {
        val fullPath = System.getProperty("user.dir")
        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled_20000.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val sc = new SparkContext(new SparkConf().setAppName("Pivot Selection Experiment"))

        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
        val rddInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            //            val classification = if (line.last == "1") "1.0" else "0.0"
            new Instance(index.toString, attributes)
        }).cache()

        val k = 100
//        val pivotsAmounts = Array(17, 20, 25, 33, 50, 100, 200)
        val pivotsAmounts = Array(300, 250, 200, 100, 50, 33, 25, 20, 17)

        val results = pivotsAmounts.map(pivotAmount => {
            val before = System.nanoTime()
            val pivots = rddInstances.take(pivotAmount)
            new PkNN(pivots, 10_000).findKNeighbors(rddInstances, k, euclidean, sc).count()
            val after = System.nanoTime()
            s"${(after - before)/ 1000000}ms"
        })

        val summary = pivotsAmounts.zip(results).map(tuple => {
            val (pivotAmount, time) = tuple
            s"With $pivotAmount that produces ${rddInstances.count() / pivotAmount} instances per pivot took $time to execute"
        }).mkString("\n")
        println(s"---------- Finished -------------\n${summary}")
    }

    def testPivotsInfluenceInExecution(): Unit = {

//        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
//        val fullPath = System.getProperty("user.dir")
//        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled_20000.csv"
//        val datasetPath = s"${fullPath}\\${datasetRelativePath}"
//
//        val sc = new SparkContext(new SparkConf().setAppName("Pivot Selection Experiment"))
//
//        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
//        val rddInstances = rawData.zipWithIndex.map(tuple => {
//            val (line, index) = tuple
//            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            //            val classification = if (line.last == "1") "1.0" else "0.0"
//            new Instance(index.toString, attributes)
//        }).cache()
//
//        val instances = rddInstances.collect()
//
//        val pivotsAmount = 50
//        val k = 100
//
//        val randomCoefficients = new ArrayBuffer[Double]()
//        val randomTimes = new ArrayBuffer[Long]()
//
//        val balancedCoefficients = new ArrayBuffer[Double]()
//        val balancedTimes = new ArrayBuffer[Long]()
//
//        val incrementalCoefficients = new ArrayBuffer[Double]()
//        val incrementalTimes = new ArrayBuffer[Long]()
//
//        val farthestCoefficients = new ArrayBuffer[Double]()
//        val farthestTimes = new ArrayBuffer[Long]()
//
//
//        for(i <- 0 until 5){
//
//            // Find random pivots
//            // Run whole search and measure time
//            val randomPivots = rddInstances.takeSample(false, pivotsAmount)
//            val beforeRandom = System.nanoTime
//            val randomCoefficientOfVariation = new PkNN(randomPivots, 10_000).findKNeighborsExperiment(rddInstances, k, euclidean, sc)
//            val afterRandom = System.nanoTime
//            randomCoefficients.addOne(randomCoefficientOfVariation)
//            randomTimes.addOne((afterRandom - beforeRandom) / 1000000)
//
//            // Find balancing pivots
//            // Run whole search and measure time
//            val sampled = rddInstances.takeSample(false, 500)
//            val objectSet = sc.parallelize(sampled.slice(0, 400))
//            val candidatePivots = sc.parallelize(sampled.slice(400, sampled.length))
//            val balancedPivots = new BalancingPivotsOccurrences(objectSet, candidatePivots, euclidean).findPivots(pivotsAmount, sc).collect()
//            val beforeBalanced = System.nanoTime
//            val balancedCoefficientOfVariation = new PkNN(balancedPivots, 10_000).findKNeighborsExperiment(rddInstances, k, euclidean, sc)
//            val afterBalanced = System.nanoTime
//            balancedCoefficients.addOne(balancedCoefficientOfVariation)
//            balancedTimes.addOne((afterBalanced - beforeBalanced) / 1000000)
//
//            // Find persistent pivots
//            // Run whole search and measure time
//            //        val persistentPivots = new PersistentRandom(15, instances).findPivots(instances, euclidean, pivotsAmount)
//            //        val beforePersistent = System.nanoTime
//            //        val persistentCoefficientOfVariation = new PkNN(persistentPivots).findKNeighborsExperiment(rddInstances, k, euclidean, sc)
//            //        val afterPersistent = System.nanoTime
//
//            // Find Farthest selection pivots
//            // Run whole search and measure time
//            val farthestObjectSet = rddInstances.takeSample(false, 500)
//            val farthestPivots = new FarthestFirstTraversal(farthestObjectSet).findPivots(instances, euclidean, pivotsAmount)
//            val beforeFarthest = System.nanoTime
//            val farthestCoefficientOfVariation = new PkNN(farthestPivots, 10_000).findKNeighborsExperiment(rddInstances, k, euclidean, sc)
//            val afterFarthest = System.nanoTime
//            farthestCoefficients.addOne(farthestCoefficientOfVariation)
//            farthestTimes.addOne((afterFarthest - beforeFarthest) / 1000000)
//
//            // Find Incremental selection pivots
//            // Run whole search and measure time
//            val incrementalSampled = rddInstances.takeSample(false, 900)
//            val incrementalObjects = incrementalSampled.slice(0, 800)
//            val incrementalObjectSet = incrementalObjects.slice(0, 400).zip(incrementalObjects.slice(400, 800))
//            val incrementalCandidatePivots = incrementalSampled.slice(800, incrementalSampled.length)
//            val incrementalPivots = new IncrementalSelection(incrementalCandidatePivots, incrementalObjectSet).findPivots(instances, euclidean, pivotsAmount)
//            val beforeIncremental = System.nanoTime
//            val incrementalCoefficientOfVariation = new PkNN(incrementalPivots, 10_000).findKNeighborsExperiment(rddInstances, k, euclidean, sc)
//            val afterIncremental = System.nanoTime
//            incrementalCoefficients.addOne(incrementalCoefficientOfVariation)
//            incrementalTimes.addOne((afterIncremental - beforeIncremental) / 1000000)
//        }
//
//        println(s"-------- Random Selection ----------\nTime: ${randomTimes.map(time => s"${time}ms").mkString(", ")}\nCoefficient of Variance: ${randomCoefficients.mkString(", ")}")
//        println(s"\n-------- Balanced Selection ----------\nTime: ${balancedTimes.map(time => s"${time}ms").mkString(", ")}\nCoefficient of Variance: ${balancedCoefficients.mkString(", ")}")
////        println(s"\n-------- Persistent Selection ----------\nTime: ${(afterPersistent - beforePersistent) / 1000000}ms\nCoefficient of Variance: ${persistentCoefficientOfVariation}")
//        println(s"\n-------- Farthest Selection ----------\nTime: ${farthestTimes.map(time => s"${time}ms").mkString(", ")}\nCoefficient of Variance: ${farthestCoefficients.mkString(", ")}")
//        println(s"\n-------- Incremental Selection ----------\nTime: ${incrementalTimes.map(time => s"${time}ms").mkString(", ")}\nCoefficient of Variance: ${incrementalCoefficients.mkString(", ")}")
    }

    def testPivotSelectionTechniques(): Unit = {

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled_50000.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Pivot Selection Experiment"))

        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
        val rddInstances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
//            val classification = if (line.last == "1") "1.0" else "0.0"
            new Instance(index.toString, attributes)
        }).cache()

        val instances = rddInstances.takeSample(false, 5000)

        // Let's cooook!!!

        // Por cada metodo de seleccion de pivotes voy a correr 100 veces
        // calculando la desviacion estandar cada vez y hayando el promedio
        // al final

        val pivotsAmount = 50
        val candidatePivotsAmount = 100
        val objectSetLength = 400


//        // Balancing Occurrences Selection
//        val balancingSelectionResults = new ArrayBuffer[Double]()
//        println("------ Balancing Occurrences Selection ------")
//        val balancingSelectionBefore = System.nanoTime
//        for(i <- 0 until 10){
//            println(s"${i * 10}% completed")
//            val sampledPivots = getRandomInstances(instances, candidatePivotsAmount + objectSetLength)
//            val candidatePivots = sampledPivots.slice(0, candidatePivotsAmount)
//            val objectSet = sampledPivots.slice(candidatePivotsAmount, sampledPivots.length)
//            println("Got random instances")
//
//            val selector = new BalancingPivotsOccurrences(sc.parallelize(objectSet), sc.parallelize(candidatePivots), euclidean, 0.1)
//            val balancedPivots = selector.findPivots(pivotsAmount, sc).collect()
//            println("Got pivots")
//            balancingSelectionResults.addOne(findPivotsCoefficientOfVariation(instances, balancedPivots, euclidean))
//        }
//        val balancingSelectionAfter = System.nanoTime
//
//        sc.stop()
//
//        // Random
//        val randomSelectionResults = new ArrayBuffer[Double]()
//        println("------ Random Selection ------")
//        val randomSelectionBefore = System.nanoTime
//        for(i <- 0 until 10){
//            println(s"${i * 10}% completed")
//            val randomPivots = getRandomInstances(instances, pivotsAmount)
//            println("Got random instances")
//            println("Got pivots")
//            randomSelectionResults.addOne(findPivotsCoefficientOfVariation(instances, randomPivots, euclidean))
//        }
//        val randomSelectionAfter = System.nanoTime
//
//
////        // Incremental Selection
//        println("------ Incremental Selection ------")
//        val incrementalSelectionResults = new ArrayBuffer[Double]()
//        val incrementalSelectionBefore = System.nanoTime
//        for(i <- 0 until 10){
//            println(s"${i * 10}% completed")
//            val sampledPivots = getRandomInstances(instances, candidatePivotsAmount + objectSetLength * 2)
//            val candidatePivots = sampledPivots.slice(0, candidatePivotsAmount)
//            val objectSetInstances = sampledPivots.slice(candidatePivotsAmount, sampledPivots.length)
//            val objectPairs = objectSetInstances.slice(0, objectSetLength).zip(objectSetInstances.slice(objectSetLength, objectSetInstances.length))
//            println("Got random instances")
//
//            val selector = new IncrementalSelection(candidatePivots, objectPairs)
//            val incrementalPivots = selector.findPivots(instances, euclidean, pivotsAmount)
//            println("Got pivots")
//            incrementalSelectionResults.addOne(findPivotsCoefficientOfVariation(instances, incrementalPivots, euclidean))
//        }
//        val incrementalSelectionAfter = System.nanoTime


//        // Farthest First Selection
//        val farthestSelectionResults = new ArrayBuffer[Double]()
//        println("------ Farthest First Selection ------")
//        val farthestSelectionBefore = System.nanoTime
//        for(i <- 0 until 10){
//            println(s"${i * 10}% completed")
//            val sampledObjects = getRandomInstances(instances, objectSetLength * 2)
//            println("Got random instances")
//
//            val selector = new FarthestFirstTraversal(sampledObjects)
//            val farthestPivots = selector.findPivots(instances, euclidean, pivotsAmount)
//            println("Got pivots")
//            farthestSelectionResults.addOne(findPivotsCoefficientOfVariation(instances, farthestPivots, euclidean))
//        }
//        val farthestSelectionAfter = System.nanoTime



        // Persistent Random Selection
        val persistentSelectionResults = new ArrayBuffer[Double]()
        println("------ Persistent Random Selection ------")
        val persistentSelectionBefore = System.nanoTime
        for(i <- 0 until 10){
            println(s"${i * 10}% completed")
            val sampledObjects = getRandomInstances(instances, objectSetLength)
            println("Got random instances")

            val selector = new PersistentRandom(50, sampledObjects)
            val persistentPivots = selector.findPivots(instances, euclidean, pivotsAmount)
            println("Got pivots")
            persistentSelectionResults.addOne(findPivotsCoefficientOfVariation(instances, persistentPivots, euclidean))
        }
        val persistentSelectionAfter = System.nanoTime

//        println(s"Average std for random selection was ${randomSelectionResults.sum.toDouble / randomSelectionResults.length.toDouble}")
//        println(s"Average execution time ${(randomSelectionAfter - randomSelectionBefore) / 1000000}ms")
//        println(s"Random STDs: ${randomSelectionResults.mkString(", ")}")
//        println(s"\nAverage std for incremental selection was ${incrementalSelectionResults.sum.toDouble / incrementalSelectionResults.length.toDouble}")
//        println(s"Average execution time ${(incrementalSelectionAfter - incrementalSelectionBefore) / 1000000}ms")
//        println(s"Incremental STDs: ${incrementalSelectionResults.mkString(", ")}")
//        println(s"\nAverage std for farthest first selection was ${farthestSelectionResults.sum.toDouble / farthestSelectionResults.length.toDouble}")
//        println(s"Average execution time ${(farthestSelectionAfter - farthestSelectionBefore) / 1000000}ms")
//        println(s"Farthest STDs: ${farthestSelectionResults.mkString(", ")}")
//        println(s"\nAverage std for balancing selection was ${balancingSelectionResults.sum.toDouble / balancingSelectionResults.length.toDouble}")
//        println(s"Average execution time ${(balancingSelectionAfter - balancingSelectionBefore) / 1000000}ms")
//        println(s"Balancing STDs: ${balancingSelectionResults.mkString(", ")}")
        println(s"\nAverage std for persistent random selection was ${persistentSelectionResults.sum.toDouble / persistentSelectionResults.length.toDouble}")
        println(s"Average execution time ${(persistentSelectionAfter - persistentSelectionBefore) / 1000000}ms")
        println(s"Persistent STDs: ${persistentSelectionResults.mkString(", ")}") }

    def findPivotsCoefficientOfVariation[A](instances: Array[Instance], pivots: Array[Instance], distanceFunction: DistanceFunction): Double = {
//        val cellsLengths = instances.cartesian(pivots)
//            .map(tuple => {
//                val (instance, pivot) = tuple
//                (instance, (pivot, distanceFunction(instance.data, pivot.data)))
//            })
//            .reduceByKey((pivotDist1, pivotDist2) => if(pivotDist2._2 < pivotDist1._2) pivotDist2 else pivotDist1)
//            .map(t => (t._2._1, (t._1, t._2._2)))
//            .groupByKey
//            .map(tuple => tuple._2.toArray.length).collect()

        val cells = mutable.Map[String, ArrayBuffer[Instance]]()

        pivots.foreach(pivot => {
            cells(pivot.id) = new ArrayBuffer[Instance]()
        })

        instances.foreach(instance => {
            val closestPivotId = pivots.map(pivot => (pivot.id, distanceFunction(pivot.data, instance.data))).minBy(_._2)._1
            cells(closestPivotId).addOne(instance)
        })

        val cellsLengths = cells.map(_._2.length).toArray
        val mean = cellsLengths.sum.toDouble / cellsLengths.length.toDouble
        val std = math.sqrt(cellsLengths.map(length => math.pow(length - mean, 2)).sum / cellsLengths.length)
        std / mean

    }

    def getRandomInstances[A](instances: Array[Instance], amount: Int): Array[Instance] = {
        Random.shuffle(instances.toList).toArray.take(amount)
    }

    def truncateDatabase(): Unit = {
        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "testingDatasets\\creditcardMinMaxScaled.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"
        val outputPath = s"${fullPath}\\testingDatasets\\creditcardMinMaxScaled_${20000}.csv"

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Truncate"))
        val rawData = sc.textFile(datasetPath).map(line => line.split(","))
        val instances = rawData.zipWithIndex.map(tuple => {
            val (line, index) = tuple
            val attributes = line.slice(0, line.length - 1).map(_.toDouble)
            val classification = line.last
            (new Instance(index.toString, attributes), classification)
        })

        val outliers = instances.filter(tuple => tuple._2 == "1").collect()
        val normals = instances.filter(tuple => tuple._2 != "1").collect()
        print(s"Well filtered: ${instances.count() == outliers.length + normals.length}")

        val normalsAmount = 20_000 - outliers.length
        val selectedNormals = Random.shuffle(normals.toSeq).toArray.take(normalsAmount)
        val joined = Random.shuffle(selectedNormals.concat(outliers).toSeq).toArray
        val stringified = joined.map(tuple => {
            val (instance, classification) = tuple
            s"${instance.data.mkString(",")},$classification"
        }).mkString("\n")
        ReaderWriter.writeToFile(outputPath, stringified)
    }
}
