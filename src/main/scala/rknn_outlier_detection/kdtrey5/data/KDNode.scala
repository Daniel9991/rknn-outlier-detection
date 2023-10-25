package rknn_outlier_detection.kdtrey5.data

import rknn_outlier_detection.kdtrey5.Utils._

sealed trait KDNode[K, V] {
  def id: NodeId
  def keys: Array[K]
  def lastKey: K
  def size: Int
  override def hashCode: Int = id.hashCode
}

case class KDBranch[K, V](
  val id: NodeId,
  val keys: Array[K],
  val lastKey: K,
  val nodes: Array[NodeId],
  val size: Int)
    extends KDNode[K, V] {
  override def toString =
    s"KDBranch($id, keys=${keys.mkString("{", ",", "}")}, lastKey=$lastKey, nodes=${nodes.mkString("{", ",", "}")}, size=$size)"
  override def equals(other: Any): Boolean = {
    other match {
      case other: KDBranch[K, V] =>
        this.id == other.id &&
          this.size == other.size &&
          this.lastKey == other.lastKey &&
          arrayEquals(this.keys, other.keys, size) &&
          arrayEquals(this.nodes, other.nodes, size)
      case _ => false
    }
  }
}

case class KDLeaf[K, V](val id: NodeId, val keys: Array[K], val values: Array[V], val size: Int)
    extends KDNode[K, V] {
  override def lastKey: K = lastNotNull(keys)
  override def toString =
    s"KDLeaf($id, keys=${keys.mkString("{", ",", "}")}, values=${values.mkString("{", ",", "}")}, size=$size)"
  override def equals(other: Any): Boolean = {
    other match {
      case other: KDLeaf[K, V] =>
        this.id == other.id &&
          this.size == other.size &&
          arrayEquals(this.keys, other.keys, size) &&
          arrayEquals(this.values, other.values, size)
      case _ => false
    }
  }
}
