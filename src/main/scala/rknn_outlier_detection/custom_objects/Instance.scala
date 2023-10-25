package rknn_outlier_detection.custom_objects

class Instance(val id: String, val attributes: Array[Double], val classification: String) extends Serializable{

  var kNeighbors: Array[KNeighbor] = null
  var rNeighbors: Array[Neighbor] = null
}
