package rknn_outlier_detection.shared.custom_objects

class OutlierPivot(id: String, data: Array[Double], val originPivotId: String) extends Instance(id, data) with Serializable {
    override def hashCode: Int = this.id.toInt

    override def equals(o: Any): Boolean = this.hashCode == o.hashCode

    override def toString: String = this.id
}

object OutlierPivot{
    def fromInstance(instance: Instance, originPivotId: String): OutlierPivot ={
        new OutlierPivot(instance.id, instance.data, originPivotId)
    }
}
