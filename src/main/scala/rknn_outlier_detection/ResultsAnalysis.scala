package rknn_outlier_detection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor}
import rknn_outlier_detection.shared.distance.DistanceFunctions
import rknn_outlier_detection.shared.utils.{ReaderWriter, Utils}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object ResultsAnalysis {

    def main(args: Array[String]): Unit = {
//        rocDurationAnalysis()
        varyingPivotsAnalysis()
//        knnwBigDataAnalysis()
    }

    def varyingPivotsAnalysis(): Unit = {
        // This function takes the stored results
        // Finds the average by k value for each method
        // and writes them to a csv for further analysis

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "results\\by_partition_experiments_for_pivot_amounts.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\by_partition_and_pivots_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class DataFrameColNames(
            val nodes: String,
            val datasetSize: String,
            val k: String,
            val pivotsAmount: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val prc: String,
            val searchDuration: String,
            val reverseDuration: String,
            val detectionDuration: String,
            val totalDuration: String
        )

        val dfCols = new DataFrameColNames(
            nodes="NODES",
            datasetSize="DATASET_SIZE",
            k="K",
            pivotsAmount="PIVOTS_AMOUNT",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            prc="PRC",
            searchDuration="SEARCH_DURATION",
            reverseDuration="REVERSE_DURATION",
            detectionDuration="DETECTION_DURATION",
            totalDuration="TOTAL_DURATION"
        )

        val datasetSchema = StructType(Array(
            StructField(dfCols.nodes, IntegerType, nullable = false),
            StructField(dfCols.datasetSize, StringType, nullable = false),
            StructField(dfCols.k, IntegerType, nullable = false),
            StructField(dfCols.pivotsAmount, IntegerType, nullable = false),
            StructField(dfCols.seed, IntegerType, nullable = false),
            StructField(dfCols.detectionMethod, StringType, nullable = false),
            StructField(dfCols.roc, DoubleType, nullable = false),
            StructField(dfCols.prc, DoubleType, nullable = false),
            StructField(dfCols.searchDuration, LongType, nullable = false),
            StructField(dfCols.detectionDuration, LongType, nullable = false),
            StructField(dfCols.totalDuration, LongType, nullable = false),
        ))

        val df = spark.read.format("csv")
            .option("header", "false")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        // By node and by k value
        val detectionMethods = Array("antihub", "ranked", "refined")
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800)
        val pivots = Array(12, 17, 25)
        val contents = new ArrayBuffer[String]()
        contents += "Roc experiments"
        contents += "\n"

        // Line plots of rocs for each node amount
        for(pivot <- pivots){
            contents += s"${pivot} ${if(pivot == 1) "pivot" else "pivots"}"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivot
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === method
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n\n\n"

        // Line plots of durations for each node amount
        for(pivot <- pivots){
            contents += s"${pivot} ${if(pivot == 1) "pivot" else "pivots"}"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivot
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === method
                    )

                    val average = findAverageDuration(values.select(dfCols.totalDuration), dfCols.totalDuration)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n\n\n"

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }

    def rocDurationAnalysis(): Unit = {
        // This function takes the stored results
        // Finds the average by k value for each method
        // and writes them to a csv for further analysis

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "results\\by_partition_experiments_clean.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\by_partition_roc_duration_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class DataFrameColNames(
            val nodes: String,
            val datasetSize: String,
            val k: String,
            val pivotsAmount: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val prc: String,
            val searchDuration: String,
            val reverseDuration: String,
            val detectionDuration: String,
            val totalDuration: String
        )

        val dfCols = new DataFrameColNames(
            nodes="NODES",
            datasetSize="DATASET_SIZE",
            k="K",
            pivotsAmount="PIVOTS_AMOUNT",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            prc="PRC",
            searchDuration="SEARCH_DURATION",
            reverseDuration="REVERSE_DURATION",
            detectionDuration="DETECTION_DURATION",
            totalDuration="TOTAL_DURATION"
        )

        val datasetSchema = StructType(Array(
            StructField(dfCols.nodes, IntegerType, nullable = false),
            StructField(dfCols.datasetSize, StringType, nullable = false),
            StructField(dfCols.k, IntegerType, nullable = false),
            StructField(dfCols.pivotsAmount, IntegerType, nullable = false),
            StructField(dfCols.seed, IntegerType, nullable = false),
            StructField(dfCols.detectionMethod, StringType, nullable = false),
            StructField(dfCols.roc, DoubleType, nullable = false),
            StructField(dfCols.prc, DoubleType, nullable = false),
            StructField(dfCols.searchDuration, LongType, nullable = false),
            StructField(dfCols.detectionDuration, LongType, nullable = false),
            StructField(dfCols.totalDuration, LongType, nullable = false),
        ))

        val df = spark.read.format("csv")
            .option("header", "false")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        // By node and by k value
        val detectionMethods = Array("antihub", "ranked", "refined")
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800, 1000)
        val nodes = Array(1, 2, 3)
        val contents = new ArrayBuffer[String]()
        contents += "Roc experiments"
        contents += "\n"

        // Line plots of rocs for each node amount
        for(node <- nodes){
            contents += s"${node} ${if(node == 1) "node" else "nodes"}"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.nodes) === node
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === method
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n\n\n"

        // Line plots of durations for each node amount
        for(node <- nodes){
            contents += s"${node} ${if(node == 1) "node" else "nodes"}"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.nodes) === node
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === method
                    )

                    val average = findAverageDuration(values.select(dfCols.totalDuration), dfCols.totalDuration)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n\n\n"

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }

    def smallDataDurationAnalysis(): Unit = {

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"

        val byPartitionDatasetRelativePath = "results\\by_partition_experiments_clean.csv"
        val byPartitionDatasetPath = s"${fullPath}\\${byPartitionDatasetRelativePath}"

        val smallDataDatasetRelativePath = "results\\small_data_experiments.csv"
        val smallDataDatasetPath = s"${fullPath}\\${smallDataDatasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\small_data_duration_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class ByPartitionDataFrameColNames(
            val nodes: String,
            val datasetSize: String,
            val k: String,
            val pivotsAmount: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val prc: String,
            val searchDuration: String,
            val reverseDuration: String,
            val detectionDuration: String,
            val totalDuration: String
        )

        val byPartitionDfCols = new ByPartitionDataFrameColNames(
            nodes="NODES",
            datasetSize="DATASET_SIZE",
            k="K",
            pivotsAmount="PIVOTS_AMOUNT",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            prc="PRC",
            searchDuration="SEARCH_DURATION",
            reverseDuration="REVERSE_DURATION",
            detectionDuration="DETECTION_DURATION",
            totalDuration="TOTAL_DURATION"
        )

        val byPartitionDatasetSchema = StructType(Array(
            StructField(byPartitionDfCols.nodes, IntegerType, nullable = false),
            StructField(byPartitionDfCols.datasetSize, StringType, nullable = false),
            StructField(byPartitionDfCols.k, IntegerType, nullable = false),
            StructField(byPartitionDfCols.pivotsAmount, IntegerType, nullable = false),
            StructField(byPartitionDfCols.seed, IntegerType, nullable = false),
            StructField(byPartitionDfCols.detectionMethod, StringType, nullable = false),
            StructField(byPartitionDfCols.roc, DoubleType, nullable = false),
            StructField(byPartitionDfCols.prc, DoubleType, nullable = false),
            StructField(byPartitionDfCols.searchDuration, LongType, nullable = false),
            StructField(byPartitionDfCols.detectionDuration, LongType, nullable = false),
            StructField(byPartitionDfCols.totalDuration, LongType, nullable = false),
        ))

        class SmallDataDataFrameColNames(
            val k: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val totalDuration: String
        )

        val smallDataDfCols = new SmallDataDataFrameColNames(
            k="K",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            totalDuration="TOTAL_DURATION"
        )

        val smallDataDatasetSchema = StructType(Array(
            StructField(smallDataDfCols.k, IntegerType, nullable = false),
            StructField(smallDataDfCols.seed, IntegerType, nullable = false),
            StructField(smallDataDfCols.detectionMethod, StringType, nullable = false),
            StructField(smallDataDfCols.roc, DoubleType, nullable = false),
            StructField(smallDataDfCols.totalDuration, LongType, nullable = false),
        ))

        val byPartitionDf = spark.read.format("csv")
            .option("header", "false")
            .schema(byPartitionDatasetSchema)
            .load(byPartitionDatasetPath)
            .cache();

        val smallDataDf = spark.read.format("csv")
            .option("header", "true")
            .schema(smallDataDatasetSchema)
            .load(smallDataDatasetPath)
            .cache();

        // By node and by k value
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800, 1000)
        val nodes = Array(1, 2, 3)
        val contents = new ArrayBuffer[String]()
        contents += s"k,small_data,1_node,2_nodes,3_nodes"

        // Line plots of rocs for each node amount
        for(k <- kValues){

            val smallDataValues = smallDataDf.filter(
                col(smallDataDfCols.k) === k
                && col(smallDataDfCols.detectionMethod) === "antihub"
            )
            val smallDataAverage = findAverageDuration(smallDataValues.select(smallDataDfCols.totalDuration), smallDataDfCols.totalDuration)

            val nodesAverages = new ArrayBuffer[Double]()
            for(node <- nodes){
                val values = byPartitionDf.filter(
                    col(byPartitionDfCols.nodes) === node
                        && col(byPartitionDfCols.k) === k
                        && col(byPartitionDfCols.detectionMethod) === "antihub"
                )

                val average = findAverageDuration(values.select(byPartitionDfCols.totalDuration), byPartitionDfCols.totalDuration)
                nodesAverages += average
            }
            contents += s"$k,$smallDataAverage,${nodesAverages.mkString(",")}"
        }

        contents += "\n\n\n\n\n"

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }


    def knnwBigDataAnalysis(): Unit = {
        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "results\\knnw_big_data_results.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\knnw_bigdata_roc_duration_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class DataFrameColNames(
            val k: String,
            val p: String,
            val partitions: String,
            val roc: String,
            val prc: String,
            val totalDuration: String
        )

        val dfCols = new DataFrameColNames(
            k="K",
            p="P",
            partitions="PARTITIONS",
            roc="ROC",
            prc="PRC",
            totalDuration="TOTAL_DURATION"
        )

        val datasetSchema = StructType(Array(
            StructField(dfCols.k, IntegerType, nullable = false),
            StructField(dfCols.p, DoubleType, nullable = false),
            StructField(dfCols.partitions, IntegerType, nullable = false),
            StructField(dfCols.roc, DoubleType, nullable = false),
            StructField(dfCols.prc, DoubleType, nullable = false),
            StructField(dfCols.totalDuration, LongType, nullable = false),
        ))

        val df = spark.read.format("csv")
            .option("header", "false")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        // By node and by k value
        val detectionMethods = Array("antihub", "ranked", "refined")
        val kValues = Array(1, 5, 10, 15, 25, 30)
        val ps = Array(0.1, 0.2)
        val partitions = Array(8, 20 , 24)
        val contents = new ArrayBuffer[String]()
        contents += "Roc experiments"
        contents += "\n"

        // Line plots of rocs for each node amount
        for(partition <- partitions){
            contents += s"${partition} partitions"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(p <- ps){
                    val values = df.filter(
                        col(dfCols.partitions) === partition
                            && col(dfCols.k) === k
                            && col(dfCols.p) === p
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n\n\n"

        for(partition <- partitions){
            contents += s"${partition} partitions"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(p <- ps){
                    val values = df.filter(
                        col(dfCols.partitions) === partition
                            && col(dfCols.k) === k
                            && col(dfCols.p) === p
                    )

                    val average = findAverageDuration(values.select(dfCols.totalDuration), dfCols.totalDuration)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }

//    def completeKNNsVersionAnalysis = ???

    def prevAnalysis(args: Array[String]): Unit = {

        val fullPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection"
        val datasetRelativePath = "results\\structured-results.csv"
        val datasetPath = s"${fullPath}\\${datasetRelativePath}"

        val experimentsFullPath = s"C:\\Users\\danny\\OneDrive\\Escritorio\\school\\la tesis\\new_experiments.csv"

        val spark = SparkSession.builder()
            .appName("Analyzing Results")
            .master("local[*]")
            .getOrCreate();

        class DataFrameColNames(
            val datasetSize: String,
            val k: String,
            val pivotsAmount: String,
            val searchMethod: String,
            val seed: String,
            val detectionMethod: String,
            val roc: String,
            val prc: String,
            val searchDuration: String,
            val reverseDuration: String,
            val detectionDuration: String,
            val totalDuration: String
        )

        val dfCols = new DataFrameColNames(
            datasetSize="DATASET_SIZE",
            k="K",
            pivotsAmount="PIVOTS_AMOUNT",
            searchMethod="SEARCH_METHOD",
            seed="SEED",
            detectionMethod="DETECTION_METHOD",
            roc="ROC",
            prc="PRC",
            searchDuration="SEARCH_DURATION",
            reverseDuration="REVERSE_DURATION",
            detectionDuration="DETECTION_DURATION",
            totalDuration="TOTAL_DURATION"
        )

        val datasetSchema = StructType(Array(
            StructField(dfCols.datasetSize, StringType, nullable = false),
            StructField(dfCols.k, IntegerType, nullable = false),
            StructField(dfCols.pivotsAmount, IntegerType, nullable = false),
            StructField(dfCols.searchMethod, StringType, nullable = false),
            StructField(dfCols.seed, IntegerType, nullable = false),
            StructField(dfCols.detectionMethod, StringType, nullable = false),
            StructField(dfCols.roc, DoubleType, nullable = false),
            StructField(dfCols.prc, DoubleType, nullable = false),
            StructField(dfCols.searchDuration, LongType, nullable = false),
            StructField(dfCols.reverseDuration, LongType, nullable = false),
            StructField(dfCols.detectionDuration, LongType, nullable = false),
            StructField(dfCols.totalDuration, LongType, nullable = false),
        ))

        val df = spark.read.format("csv")
            .option("header", "true")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        df.filter(col(dfCols.roc) > 0.9).orderBy(desc(dfCols.roc)).show(20)

        val detectionMethods = Array("antihub", "ranked", "refined")
        val kValues = Array(1, 5, 10, 25, 50, 100, 200, 400, 600, 800, 1000)
        val pivotsAmounts = Array(12, 25, 50, 100)
        val contents = new ArrayBuffer[String]()

        // Line plots of rocs for each pivots amount
        for(pivots <- pivotsAmounts){
            contents += s"${pivots} pivots"
            contents += s"k,${detectionMethods.mkString(",")}"

            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(method <- detectionMethods){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                        && col(dfCols.k) === k
                        && col(dfCols.detectionMethod) === method
                        && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"

        // Line plots of rocs varying pivotsAmount for each detection technique
        for(detectionMethod <- detectionMethods){
            contents += s"roc by pivotsAmount ($detectionMethod)"
            contents += s"k,${pivotsAmounts.mkString(",")}"
            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(pivots <- pivotsAmounts){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === detectionMethod
                            && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"

        // Line plots of time(duration) varying pivotsAmount for each detection technique
        for(detectionMethod <- detectionMethods){
            contents += s"total_duration by pivotsAmount ($detectionMethod)"
            contents += s"k,${pivotsAmounts.mkString(",")}"
            for(k <- kValues){
                val averages = new ArrayBuffer[Double]()
                for(pivots <- pivotsAmounts){
                    val values = df.filter(
                        col(dfCols.pivotsAmount) === pivots
                            && col(dfCols.k) === k
                            && col(dfCols.detectionMethod) === detectionMethod
                            && col(dfCols.searchMethod) === "classic"
                    )

                    val average = findAverageDuration(values.select(dfCols.totalDuration), dfCols.totalDuration)
                    averages += average
                }
                contents += s"$k,${averages.mkString(",")}"
            }
            contents += "\n"
        }

        contents += "\n\n\n"


        //Line plot of time(duration) of search phase
        contents += s"search duration by pivots"
        contents += s"k,${pivotsAmounts.mkString(",")}"
        for(k <- kValues){
            val averages = new ArrayBuffer[Double]()
            for(pivots <- pivotsAmounts){
                val values = df.filter(
                    col(dfCols.pivotsAmount) === pivots
                        && col(dfCols.k) === k
                        && col(dfCols.searchMethod) === "classic"
                )

                val average = findAverageDuration(values.select(dfCols.searchDuration), dfCols.searchDuration)
                averages += average
            }
            contents += s"$k,${averages.mkString(",")}"
        }

        contents += "\n\n\n"

        // Line plot of rocs for exhaustive search
        contents += "exhaustive"
        contents += s"k,${detectionMethods.mkString(",")}"
        for(k <- kValues){
            val averages = new ArrayBuffer[Double]()
            for(method <- detectionMethods){
                val values = df.filter(
                    col(dfCols.k) === k
                    && col(dfCols.detectionMethod) === method
                    && col(dfCols.searchMethod) === "exhaustive"
                )

                val average = findAverageRoc(values.select(dfCols.roc), dfCols.roc)
                averages += average
            }
            contents += s"$k,${averages.mkString(",")}"
        }

        ReaderWriter.writeToFile(experimentsFullPath, contents.mkString("\n"))
    }

    def findAverageRoc(df: DataFrame, rocColName: String): Double = {
        val length = df.count()
        df.select(rocColName).rdd.map(r => r.getDouble(0)).reduce(_+_) / length.toDouble
    }

    def findAverageDuration(df: DataFrame, rocColName: String): Double = {
        val length = df.count()
        df.select(rocColName).rdd.map(r => r.getLong(0) / 1000).reduce(_+_).toDouble / length.toDouble
    }
}
