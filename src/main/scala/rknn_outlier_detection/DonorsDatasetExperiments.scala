package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import rknn_outlier_detection.big_data.search.pivot_based.{FarthestFirstTraversal, IncrementalSelection}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}

import scala.collection.mutable.ArrayBuffer

object DonorsDatasetExperiments {

    def main(args: Array[String]): Unit = {

        val pivotsAmount = 206
        // 87654 458212 57124 6745 12541 7634 431 90783 34634 56342
        val seed = scala.util.Random.nextInt()
        val distanceFunction = euclidean

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetRelativePath = s"testingDatasets\\donors.csv"
            val datasetPath = s"${fullPath}\\${datasetRelativePath}"

            val config = new SparkConf()
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))
            config.setMaster("local[*]")
            config.set("spark.default.parallelism", "48")

            val spark = SparkSession.builder()
                .config(config)
                .appName(s"Donors dataset")
                .getOrCreate();

            val sc = spark.sparkContext

            import spark.implicits._

            val rawData = spark.read.textFile(datasetPath).map(row => row.split(","))
            val instancesAndClassification = rawData.rdd.zipWithIndex.map{case (line, index) => {
                val attributes = line.slice(0, line.length - 1).map(_.toDouble)
                val classification = if (line.last == "1") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.persist()

            val instances = instancesAndClassification.map(_._1)

//            val sample = instances.takeSample(withReplacement = false, pivotsAmount, seed)
//            val pivots = sc.broadcast(sample)
//
//            if(sample.length != pivotsAmount)
//                throw new Exception("There was a different amount of sampled pivots than required")

//            val objectSet = sc.parallelize(instances.takeSample(withReplacement = false, seed=seed, num=6190), sc.defaultParallelism)
//
//            val pivotsBuffer = new FarthestFirstTraversal(seed=seed).findPivots(objectSet, pivotsAmount, distanceFunction)
//
//            val pivots = sc.broadcast(pivotsBuffer)

            val sample = instances.takeSample(withReplacement=false, num=2500)
            val candidatePivots = sc.parallelize(sample.slice(0, 500))
            val objectPairs = sc.parallelize(sample.slice(500, 1500).zip(sample.slice(1500, 2500)))
            val selectedPivots = new IncrementalSelection().findPivots(candidatePivots, objectPairs, pivotsAmount, distanceFunction, sc)
            val pivots = sc.broadcast(selectedPivots)

            // Create cells
            val cells = instances.map(instance => {
                val closestPivot = pivots.value
                    .map(pivot => (pivot, distanceFunction(pivot.data, instance.data)))
                    .reduce {(pair1, pair2) => if(pair1._2 <= pair2._2) pair1 else pair2 }

                (closestPivot._1, instance)
            })

            val countByPivot = cells.map{case (pivot, instance) => (pivot.id, 1)}.reduceByKey(_+_)

            println(s"Initial pivots: ${selectedPivots.length}")
            println(s"Pivots with more than 0 points: ${countByPivot.count()}")
            println(s"${countByPivot.sortBy(_._2,ascending = false).collect().mkString("\n", ",\n", "\n")}")

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }
}
