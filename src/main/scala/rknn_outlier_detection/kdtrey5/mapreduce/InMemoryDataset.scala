package rknn_outlier_detection.kdtrey5.mapreduce

import scala.collection._
import scala.reflect.ClassTag

object InMemoryDataset {
  def apply[T](iter: Iterable[T]): InMemoryDataset[T] = new InMemoryDataset(iter)
}

class InMemoryDataset[T](iter: => Iterable[T]) extends Dataset[T] {
  private lazy val _iter = iter
  override def cache(): Unit = {
    _iter.toSeq
  }
  override def filter(f: T => Boolean): Dataset[T] = {
    new InMemoryDataset(_iter filter f)
  }
  override def map[U](f: T => U)(implicit arg0: ClassTag[U]): Dataset[U] = {
    new InMemoryDataset(_iter map f)
  }
  override def flatmap[U](f: T => Iterator[U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    new InMemoryDataset(new FlatmapIterable[T, U](_iter, f))
  }
  override def mapPartitions[TT >: T, U](f: (Int, Int) => PartitionMapper[TT, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    mapPartition(f(0, 1))
  }
  override def mapPartition[U](f: PartitionMapper[T, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    new InMemoryDataset({
      val buffer = new mutable.ArrayBuffer[U]()
      def append(u: U) = buffer.append(u)
      f.mapPartition(_iter.iterator, append)
      buffer
    })
  }
  override def toSeq: Seq[T] = _iter.toSeq
  override def sorted[TT >: T](implicit ordering: Ordering[TT], arg0: ClassTag[TT]): Dataset[T] = {
    new InMemoryDataset(_iter.toSeq sorted ordering)
  }
  override def size: Long = _iter.size
  override def append[TT >: T](ds: Dataset[TT])(implicit arg0: ClassTag[TT]): Dataset[TT] = {
    new PartitionedDataset({ Seq(this, ds) })
  }
}
