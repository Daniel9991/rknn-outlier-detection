package rknn_outlier_detection.big_data.partitioners

import org.apache.spark.Partitioner
import rknn_outlier_detection.shared.custom_objects.Instance

class PivotsPartitioner(numParts: Int, pivotsIds: Array[Int]) extends Partitioner with Serializable{
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = {
        key match {
            case k: Instance => pivotsIds.indexOf(k.id)
            case id: Int => pivotsIds.indexOf(id)
        }
    }
}
