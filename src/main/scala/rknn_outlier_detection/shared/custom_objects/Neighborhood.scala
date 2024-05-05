package rknn_outlier_detection.shared.custom_objects

import org.apache.spark.rdd.RDD

class Neighborhood(val pivot: Instance, val instances: RDD[Instance])
