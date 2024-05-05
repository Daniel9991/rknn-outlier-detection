package rknn_outlier_detection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SparkSession}

object SparkInAction {

    def main(args: Array[String]): Unit ={

//        val spark = SparkSession.builder()
//            .appName("Restaurants in Wake County, NC")
//            .master("local[*]")
//            .getOrCreate();
//
//        var df = spark.read.format("csv")
//            .option("header", "false")
//            .load("datasets/iris.csv");
//
//        println("*** Right after ingestion");
//        df.show(5);
//        df.printSchema()
//
//        println("*** Looking at partitions")
//        val partitions = df.rdd.partitions
//        val partitionCount = partitions.length
//        println("Partition count before repartition: " + partitionCount)
//
//        df = df.repartition(4)
//        println("Partition count after repartition: " + df.rdd.partitions.length)
//
//        df = df.drop("_c4")
//        val rdd = df.rdd
////        df = df.map(row => row.toSeq.map())
//
//        val everything = rdd.collect()
//        print(everything.map(row => row(0).toString.toDouble).sum)
//        df.printSchema()

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Sparking2"))
        val nums = sc.parallelize(Seq(1, 2, 3, 4, 5))
        val collected = nums.collect().sum
        println(collected)
    }
}
