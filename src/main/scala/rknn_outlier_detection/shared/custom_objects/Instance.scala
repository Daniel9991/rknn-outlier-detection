package rknn_outlier_detection.shared.custom_objects

class Instance[A](val id: String, val attributes: A, val classification: String) extends Serializable{

  var kNeighbors: Array[KNeighbor] = null
  var rNeighbors: Array[RNeighbor] = null
  var antihubScore: Double = 0.0

  override def hashCode: Int = this.id.toInt

  override def equals(o: Any): Boolean = this.hashCode == o.hashCode

  override def toString: String = this.id
}
