package rknn_outlier_detection

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max, min, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import rknn_outlier_detection.BigDataExperiment.saveStatistics
import rknn_outlier_detection.big_data.alternative_methods.SameThingByPartition
import rknn_outlier_detection.shared.custom_objects.{Instance, KNeighbor, RNeighbor}
import rknn_outlier_detection.shared.utils.ReaderWriter
import rknn_outlier_detection.small_data.detection

object WebAttacks {

    def main(args: Array[String]): Unit = {
        experiment(args)
//        preprocess(args)
    }

    def experiment(args: Array[String]): Unit = {
        val nodes = if(args.length > 0) args(0).toInt else 1
        val pivotsAmount = if(args.length > 1) args(1).toInt else 63 // 1018528 rows 973224 rows
        val k = if(args.length > 2) args(2).toInt else 200
        val seed = if(args.length > 3) args(3).toInt else 87654 // 87654
        val detectionMethod = if(args.length > 5) args(5) else "antihub"
        val distanceFunction = euclidean

        try{
            val fullPath = System.getProperty("user.dir")

            val datasetPath = s"${fullPath}\\testingDatasets\\webattacksbotScaled.csv"

            val config = new SparkConf()
            config.setMaster("local[*]")
            config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            config.registerKryoClasses(Array(classOf[KNeighbor], classOf[Instance], classOf[RNeighbor]))

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
                val classification = if (line.last != "Bot") "1.0" else "0.0"
                (Instance(index.toInt, attributes), classification)
            }}.persist()

            val instances = instancesAndClassification.map(_._1)
            val classifications = instancesAndClassification.map{case (instance, classification) => (instance.id, classification)}

            //            val customListener = new CustomSparkListener
            //            spark.sparkContext.addSparkListener(customListener)

            val sample = instances.takeSample(withReplacement = false, pivotsAmount, seed)
            val onStart = System.nanoTime()
            val outlierDegrees = new SameThingByPartition().detectAnomalies(instances, pivotsAmount, seed, k, distanceFunction, sc, detectionCriteria, sample).persist()
            outlierDegrees.count()
            val onFinish = System.nanoTime
            //            spark.sparkContext.removeSparkListener(customListener)
            val duration = (onFinish - onStart) / 1000000
            val predictionsAndLabels = classifications.join(outlierDegrees).map(tuple => (tuple._2._2, tuple._2._1.toDouble))
            val detectionMetrics = new BinaryClassificationMetrics(predictionsAndLabels)

            val line = s"$nodes,$k,$pivotsAmount,$seed,$detectionMethod,${detectionMetrics.areaUnderROC()},${detectionMetrics.areaUnderPR()},0,0,$duration"
            //            val line = s"$nodes,$k,$seed,${detectionMetrics.areaUnderROC()},${customListener.totalProcessingTime},${customListener.totalNetworkTime},$duration"
            saveStatistics(line, s"C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\results\\webattacks_experiments.csv")

            println(s"---------------Done executing-------------------")
        }
        catch{
            case e: Exception => {
                println("-------------The execution didn't finish due to------------------")
                println(e)
            }
        }
    }

    def preprocess(args: Array[String]): Unit = {
        val config = new SparkConf()
        config.setMaster("local[*]")
        config.set("spark.executor.memory", "12g")
        config.set("spark.default.parallelism", "48")

        val spark = SparkSession.builder()
            .config(config)
            .appName("Preprocessing WebAttacks")
            .getOrCreate();

        import spark.implicits._

        val datasetPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\testingDatasets\\webattacksbot.csv"
        val newDatasetPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\testingDatasets\\webattacksbotScaled.csv"

        class DataFrameColNames(val col1: String, val col2: String, val col3: String, val col4: String, val col5: String, val col6: String, val col7: String, val col8: String, val col9: String, val col10: String, val col11: String, val col12: String, val col13: String, val col14: String, val col15: String, val col16: String, val col17: String, val col18: String, val col19: String, val col20: String, val col21: String, val col22: String, val col23: String, val col24: String, val col25: String, val col26: String, val col27: String, val col28: String, val col29: String, val col30: String, val col31: String, val col32: String, val col33: String, val col34: String, val col35: String, val col36: String, val col37: String, val col38: String, val col39: String, val col40: String, val col41: String, val col42: String, val col43: String, val col44: String, val col45: String, val col46: String, val col47: String, val col48: String, val col49: String, val col50: String, val col51: String, val col52: String, val col53: String, val col54: String, val col55: String, val col56: String, val col57: String, val col58: String, val col59: String, val col60: String, val col61: String, val col62: String, val col63: String, val col64: String, val col65: String, val col66: String, val col67: String, val col68: String, val col69: String, val col70: String, val col71: String, val col72: String, val col73: String, val col74: String, val col75: String, val col76: String, val col77: String, val col78: String, val label: String)

        val dfCols = new DataFrameColNames(col1 = "col1", col2 = "col2", col3 = "col3", col4 = "col4", col5 = "col5", col6 = "col6", col7 = "col7", col8 = "col8", col9 = "col9", col10 = "col10", col11 = "col11", col12 = "col12", col13 = "col13", col14 = "col14", col15 = "col15", col16 = "col16", col17 = "col17", col18 = "col18", col19 = "col19", col20 = "col20", col21 = "col21", col22 = "col22", col23 = "col23", col24 = "col24", col25 = "col25", col26 = "col26", col27 = "col27", col28 = "col28", col29 = "col29", col30 = "col30", col31 = "col31", col32 = "col32", col33 = "col33", col34 = "col34", col35 = "col35", col36 = "col36", col37 = "col37", col38 = "col38", col39 = "col39", col40 = "col40", col41 = "col41", col42 = "col42", col43 = "col43", col44 = "col44", col45 = "col45", col46 = "col46", col47 = "col47", col48 = "col48", col49 = "col49", col50 = "col50", col51 = "col51", col52 = "col52", col53 = "col53", col54 = "col54", col55 = "col55", col56 = "col56", col57 = "col57", col58 = "col58", col59 = "col59", col60 = "col60", col61 = "col61", col62 = "col62", col63 = "col63", col64 = "col64", col65 = "col65", col66 = "col66", col67 = "col67", col68 = "col68", col69 = "col69", col70 = "col70", col71 = "col71", col72 = "col72", col73 = "col73", col74 = "col74", col75 = "col75", col76 = "col76", col77 = "col77", col78 = "col78", label = "label")

        val datasetSchema = StructType(Array(StructField(dfCols.col1, DoubleType, nullable = false), StructField(dfCols.col2, DoubleType, nullable = false), StructField(dfCols.col3, DoubleType, nullable = false), StructField(dfCols.col4, DoubleType, nullable = false), StructField(dfCols.col5, DoubleType, nullable = false), StructField(dfCols.col6, DoubleType, nullable = false), StructField(dfCols.col7, DoubleType, nullable = false), StructField(dfCols.col8, DoubleType, nullable = false), StructField(dfCols.col9, DoubleType, nullable = false), StructField(dfCols.col10, DoubleType, nullable = false), StructField(dfCols.col11, DoubleType, nullable = false), StructField(dfCols.col12, DoubleType, nullable = false), StructField(dfCols.col13, DoubleType, nullable = false), StructField(dfCols.col14, DoubleType, nullable = false), StructField(dfCols.col15, DoubleType, nullable = false), StructField(dfCols.col16, DoubleType, nullable = false), StructField(dfCols.col17, DoubleType, nullable = false), StructField(dfCols.col18, DoubleType, nullable = false), StructField(dfCols.col19, DoubleType, nullable = false), StructField(dfCols.col20, DoubleType, nullable = false), StructField(dfCols.col21, DoubleType, nullable = false), StructField(dfCols.col22, DoubleType, nullable = false), StructField(dfCols.col23, DoubleType, nullable = false), StructField(dfCols.col24, DoubleType, nullable = false), StructField(dfCols.col25, DoubleType, nullable = false), StructField(dfCols.col26, DoubleType, nullable = false), StructField(dfCols.col27, DoubleType, nullable = false), StructField(dfCols.col28, DoubleType, nullable = false), StructField(dfCols.col29, DoubleType, nullable = false), StructField(dfCols.col30, DoubleType, nullable = false), StructField(dfCols.col31, DoubleType, nullable = false), StructField(dfCols.col32, DoubleType, nullable = false), StructField(dfCols.col33, DoubleType, nullable = false), StructField(dfCols.col34, DoubleType, nullable = false), StructField(dfCols.col35, DoubleType, nullable = false), StructField(dfCols.col36, DoubleType, nullable = false), StructField(dfCols.col37, DoubleType, nullable = false), StructField(dfCols.col38, DoubleType, nullable = false), StructField(dfCols.col39, DoubleType, nullable = false), StructField(dfCols.col40, DoubleType, nullable = false), StructField(dfCols.col41, DoubleType, nullable = false), StructField(dfCols.col42, DoubleType, nullable = false), StructField(dfCols.col43, DoubleType, nullable = false), StructField(dfCols.col44, DoubleType, nullable = false), StructField(dfCols.col45, DoubleType, nullable = false), StructField(dfCols.col46, DoubleType, nullable = false), StructField(dfCols.col47, DoubleType, nullable = false), StructField(dfCols.col48, DoubleType, nullable = false), StructField(dfCols.col49, DoubleType, nullable = false), StructField(dfCols.col50, DoubleType, nullable = false), StructField(dfCols.col51, DoubleType, nullable = false), StructField(dfCols.col52, DoubleType, nullable = false), StructField(dfCols.col53, DoubleType, nullable = false), StructField(dfCols.col54, DoubleType, nullable = false), StructField(dfCols.col55, DoubleType, nullable = false), StructField(dfCols.col56, DoubleType, nullable = false), StructField(dfCols.col57, DoubleType, nullable = false), StructField(dfCols.col58, DoubleType, nullable = false), StructField(dfCols.col59, DoubleType, nullable = false), StructField(dfCols.col60, DoubleType, nullable = false), StructField(dfCols.col61, DoubleType, nullable = false), StructField(dfCols.col62, DoubleType, nullable = false), StructField(dfCols.col63, DoubleType, nullable = false), StructField(dfCols.col64, DoubleType, nullable = false), StructField(dfCols.col65, DoubleType, nullable = false), StructField(dfCols.col66, DoubleType, nullable = false), StructField(dfCols.col67, DoubleType, nullable = false), StructField(dfCols.col68, DoubleType, nullable = false), StructField(dfCols.col69, DoubleType, nullable = false), StructField(dfCols.col70, DoubleType, nullable = false), StructField(dfCols.col71, DoubleType, nullable = false), StructField(dfCols.col72, DoubleType, nullable = false), StructField(dfCols.col73, DoubleType, nullable = false), StructField(dfCols.col74, DoubleType, nullable = false), StructField(dfCols.col75, DoubleType, nullable = false), StructField(dfCols.col76, DoubleType, nullable = false), StructField(dfCols.col77, DoubleType, nullable = false), StructField(dfCols.col78, DoubleType, nullable = false), StructField(dfCols.label, StringType, nullable = false)))

        val df = spark.read.format("csv")
            .option("header", "false")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        val cols = Array("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29", "col30", "col31", "col32", "col33", "col34", "col35", "col36", "col37", "col38", "col39", "col40", "col41", "col42", "col43", "col44", "col45", "col46", "col47", "col48", "col49", "col50", "col51", "col52", "col53", "col54", "col55", "col56", "col57", "col58", "col59", "col60", "col61", "col62", "col63", "col64", "col65", "col66", "col67", "col68", "col69", "col70", "col71", "col72", "col73", "col74", "col75", "col76", "col77", "col78")

        var transformedDf = df

        for(colName <- cols){

            val minAndMax = transformedDf.agg(min(colName), max(colName))
            val minAndMaxArr = minAndMax.as[(Double, Double)].collect()
            val minimum = minAndMaxArr(0)._1
            val maximum = minAndMaxArr(0)._2

            if((minimum == 0.0 && maximum == 0.0) || maximum == Double.NaN || minimum == Double.NaN){
                transformedDf = transformedDf.drop(colName)
            }
            else{

                val scaleUdf = udf((value: Double) => (value - minimum) / (maximum - minimum))
                transformedDf = transformedDf.withColumn(s"${colName}_scaled", scaleUdf(col(colName))).drop(colName)
            }

        }

        val data = transformedDf.map(row => {
            val seq = row.toSeq
            s"${seq.slice(1, seq.length).mkString(",")},${seq.head}"
        }).rdd.collect().mkString("\n")
        ReaderWriter.writeToFile(newDatasetPath, data)
    }

    def preprocessFound(args: Array[String]): Unit = {
        val config = new SparkConf()
        config.setMaster("local[*]")
        config.set("spark.executor.memory", "12g")
        config.set("spark.default.parallelism", "48")

        val spark = SparkSession.builder()
            .config(config)
            .appName("Preprocessing KDD99")
            .getOrCreate();

        import spark.implicits._

        val datasetPath = "C:\\Users\\danny\\Downloads\\BDs\\kdd99-unsupervised-ad.csv"
        val newDatasetPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\testingDatasets\\foundKDD99Scaled.csv"

        class DataFrameColNames(val col1: String, val col2: String, val col3: String, val col4: String, val col5: String, val col6: String, val col7: String, val col8: String, val col9: String, val col10: String, val col11: String, val col12: String, val col13: String, val col14: String, val col15: String, val col16: String, val col17: String, val col18: String, val col19: String, val col20: String, val col21: String, val col22: String, val col23: String, val col24: String, val col25: String, val col26: String, val col27: String, val col28: String, val col29: String, val label: String)

        val dfCols = new DataFrameColNames(col1 = "col1", col2 = "col2", col3 = "col3", col4 = "col4", col5 = "col5", col6 = "col6", col7 = "col7", col8 = "col8", col9 = "col9", col10 = "col10", col11 = "col11", col12 = "col12", col13 = "col13", col14 = "col14", col15 = "col15", col16 = "col16", col17 = "col17", col18 = "col18", col19 = "col19", col20 = "col20", col21 = "col21", col22 = "col22", col23 = "col23", col24 = "col24", col25 = "col25", col26 = "col26", col27 = "col27", col28 = "col28", col29 = "col29", label = "label")

        val datasetSchema = StructType(Array(StructField(dfCols.col1, DoubleType, nullable = false), StructField(dfCols.col2, DoubleType, nullable = false), StructField(dfCols.col3, DoubleType, nullable = false), StructField(dfCols.col4, DoubleType, nullable = false), StructField(dfCols.col5, DoubleType, nullable = false), StructField(dfCols.col6, DoubleType, nullable = false), StructField(dfCols.col7, DoubleType, nullable = false), StructField(dfCols.col8, DoubleType, nullable = false), StructField(dfCols.col9, DoubleType, nullable = false), StructField(dfCols.col10, DoubleType, nullable = false), StructField(dfCols.col11, DoubleType, nullable = false), StructField(dfCols.col12, DoubleType, nullable = false), StructField(dfCols.col13, DoubleType, nullable = false), StructField(dfCols.col14, DoubleType, nullable = false), StructField(dfCols.col15, DoubleType, nullable = false), StructField(dfCols.col16, DoubleType, nullable = false), StructField(dfCols.col17, DoubleType, nullable = false), StructField(dfCols.col18, DoubleType, nullable = false), StructField(dfCols.col19, DoubleType, nullable = false), StructField(dfCols.col20, DoubleType, nullable = false), StructField(dfCols.col21, DoubleType, nullable = false), StructField(dfCols.col22, DoubleType, nullable = false), StructField(dfCols.col23, DoubleType, nullable = false), StructField(dfCols.col24, DoubleType, nullable = false), StructField(dfCols.col25, DoubleType, nullable = false), StructField(dfCols.col26, DoubleType, nullable = false), StructField(dfCols.col27, DoubleType, nullable = false), StructField(dfCols.col28, DoubleType, nullable = false), StructField(dfCols.col29, DoubleType, nullable = false), StructField(dfCols.label, StringType, nullable = false)))

        val df = spark.read.format("csv")
            .option("header", "false")
            .schema(datasetSchema)
            .load(datasetPath)
            .cache();

        val cols = Array("col1", "col2", "col3", "col4","col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29")

        var transformedDf = df

        for(colName <- cols){
//            // Step 1: Convert the column to a vector
//            val assembler = new VectorAssembler()
//                .setInputCols(Array(colName))
//                .setOutputCol(s"${colName}_vector")
//            transformedDf = assembler.transform(transformedDf)
//
//            // Step 2: Apply MinMaxScaler
//            val scaler = new MinMaxScaler()
//                .setMin(0)
//                .setMax(1)
//                .setInputCol(s"${colName}_vector")
//                .setOutputCol(s"${colName}_scaled")
//            val model = scaler.fit(transformedDf)
//            transformedDf = model.transform(transformedDf)
//
//            // Step 3: Drop the intermediate vector column
//            transformedDf = transformedDf.drop(colName)
//            transformedDf = transformedDf.drop(s"${colName}_vector")

            val minAndMax = transformedDf.agg(min(colName), max(colName))
            val minAndMaxArr = minAndMax.as[(Double, Double)].collect()
            val minimum = minAndMaxArr(0)._1
            val maximum = minAndMaxArr(0)._2

            if(minimum == 0.0 && maximum == 0.0){
                transformedDf = transformedDf.drop(colName)
            }
            else{

                val scaleUdf = udf((value: Double) => (value - minimum) / (maximum - minimum))
                transformedDf = transformedDf.withColumn(s"${colName}_scaled", scaleUdf(col(colName))).drop(colName)
            }

        }

        val data = transformedDf.map(row => {
            val seq = row.toSeq
            s"${seq.slice(1, seq.length).mkString(",")},${seq.head}"
        }).rdd.collect().mkString("\n")
        ReaderWriter.writeToFile(newDatasetPath, data)
    }

    def createSubSubKDD(args: Array[String]): Unit = {

        val config = new SparkConf()
        config.setMaster("local[*]")
        config.set("spark.executor.memory", "12g")
        config.set("spark.default.parallelism", "48")

        val spark = SparkSession.builder()
            .config(config)
            .appName("Preprocessing KDD99")
            .getOrCreate();

        val ogKDDPath = "C:\\Users\\danny\\Downloads\\BDs\\kdd\\kddcup.csv"
        val subKDDPath = "C:\\Users\\danny\\OneDrive\\Escritorio\\Proyectos\\scala\\rknn-outlier-detection\\testingDatasets\\subsubKDD99.csv"
        val rawLines = spark.sparkContext.textFile(ogKDDPath)
        val rows = rawLines.map(line => line.split(",")).map(row => row.slice(0, row.length - 1).concat(Array(row.last.slice(0, row.last.length - 1))))
        println(rows.map(row => (row.last, 1)).countByKey())
        val remaining = rows.filter(row => !Array("nmap", "portsweep", "smurf", "neptune", "ipsweep", "back", "teardrop", "satan", "warezclient").contains(row.last))
        println(remaining.filter(row => row.last != "normal").count)
//        val data = remaining.map(row => row.mkString(",")).collect().mkString("\n")
//        ReaderWriter.writeToFile(subKDDPath, data)
    }
}
