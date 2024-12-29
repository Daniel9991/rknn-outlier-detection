package rknn_outlier_detection.shared.custom_objects

class OutlierPivot(id: Int, data: Array[Double], val originPivotId: Int) extends Instance(id, data) with Serializable {
    override def hashCode: Int = this.id.toInt

    override def equals(o: Any): Boolean = this.hashCode == o.hashCode

    override def toString: String = this.id.toString
}

object OutlierPivot{
    def fromInstance(instance: Instance, originPivotId: Int): OutlierPivot ={
        new OutlierPivot(instance.id, instance.data, originPivotId)
    }
}
