package rknn_outlier_detection.kdtrey5.mapreduce

import scala.collection._
import scala.reflect.ClassTag

class PartitionedDataset[T](val datasets: Seq[Dataset[T]]) extends Dataset[T] {
  override def cache(): Unit = {
    datasets foreach { _.cache() }
  }
  override def filter(f: T => Boolean): Dataset[T] = {
    new PartitionedDataset(datasets map (_ filter f))
  }
  override def map[U](f: T => U)(implicit arg0: ClassTag[U]): Dataset[U] = {
    new PartitionedDataset(datasets map (_ map f))
  }
  override def flatmap[U](f: T => Iterator[U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    new PartitionedDataset(datasets map (_ flatmap f))
  }
  override def mapPartitions[TT >: T, U](f: (Int, Int) => PartitionMapper[TT, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    val newDatasets = datasets.zipWithIndex map {
      case (ds, index) => ds.mapPartition(f(index, datasets.size))
    }
    new PartitionedDataset(newDatasets)
  }
  override def mapPartition[U](f: PartitionMapper[T, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
    val newDatasets = datasets.zipWithIndex map { case (ds, index) => ds.mapPartition(f) }
    new PartitionedDataset(newDatasets)
  }
  override def toSeq: Seq[T] = {
    val result = new mutable.ArrayBuffer[T](size.toInt)
    for (d <- datasets) {
      result ++= d.toSeq
    }
    result
  }
  override def sorted[TT >: T](implicit ordering: Ordering[TT], arg0: ClassTag[TT]): Dataset[T] = {
    new InMemoryDataset(toSeq sorted ordering)
  }
  override def size: Long = { datasets.map(_.size).sum }
  override def append[TT >: T](ds: Dataset[TT])(implicit arg0: ClassTag[TT]): Dataset[TT] = {
    new PartitionedDataset(datasets :+ ds)
  }
}
