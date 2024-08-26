package rknn_outlier_detection.shared.custom_objects

class Instance(val id: String, val data: Array[Double]) extends Serializable{

  override def hashCode: Int = this.id.toInt

  override def equals(o: Any): Boolean = this.hashCode == o.hashCode

  override def toString: String = this.id
}
