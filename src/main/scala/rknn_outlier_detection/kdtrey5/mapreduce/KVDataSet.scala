package rknn_outlier_detection.kdtrey5.mapreduce

import scala.collection._
import scala.reflect.ClassTag

case class KVDataset[K, V](ds: Dataset[(K, V)]) {

  def sortByKey(implicit ordering: Ordering[K], ctag: ClassTag[(K, V)]): Dataset[(K, V)] = {
    object KVOrdering extends Ordering[(K, V)] {
      override def compare(v1: (K, V), v2: (K, V)): Int = {
        ordering.compare(v1._1, v2._1)
      }
    }
    ds.sorted(KVOrdering, ctag)
  }
}
