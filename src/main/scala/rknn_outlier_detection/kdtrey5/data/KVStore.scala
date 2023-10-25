package rknn_outlier_detection.kdtrey5.data

import scala.collection._

trait KVStore[K, V] {
  def load(id: NodeId): KDNode[K, V]
  def store(id: NodeId, node: KDNode[K, V]): Unit

  var rootId: NodeId
}

class InMemoryKVStore[K, V] extends KVStore[K, V] {

  val map: mutable.Map[NodeId, KDNode[K, V]] = mutable.Map()

  override var rootId: NodeId = _

  override def load(id: NodeId): KDNode[K, V] = {
    map(id)
  }

  override def store(id: NodeId, node: KDNode[K, V]): Unit = {
    map(id) = node
  }
}
