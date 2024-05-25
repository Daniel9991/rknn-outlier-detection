package rknn_outlier_detection.shared.custom_objects

import org.apache.spark.rdd.RDD

class Neighborhood[A](val pivot: Instance[A], val instances: RDD[Instance[A]])
