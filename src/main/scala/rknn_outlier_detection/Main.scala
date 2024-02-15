package rknn_outlier_detection

import org.apache.spark.{SparkConf, SparkContext}

object Main {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    def main(args: Array[String]): Unit ={

        val random = sc.parallelize(Seq(1,2,3,4,5))

        println("Hi, mom!")
        println(random.count())
    }
}
