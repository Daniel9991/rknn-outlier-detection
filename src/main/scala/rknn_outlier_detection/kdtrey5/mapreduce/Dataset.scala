package rknn_outlier_detection.kdtrey5.mapreduce

import scala.collection._
import scala.reflect.ClassTag

/**
 * A minimal abstraction over Spark's distributed computing facilities.
 */
trait Dataset[+T] {
  def cache(): Unit
  def count(): Long = size
  def filter(f: T => Boolean): Dataset[T]
  def map[U](f: T => U)(implicit arg0: ClassTag[U]): Dataset[U]
  def flatmap[U](f: T => Iterator[U])(implicit arg0: ClassTag[U]): Dataset[U]
  def mapPartitions[TT >: T, U](f: (Int, Int) => PartitionMapper[TT, U])(implicit arg0: ClassTag[U]): Dataset[U]
  def mapPartition[U](f: PartitionMapper[T, U])(implicit arg0: ClassTag[U]): Dataset[U]
  def toSeq: Seq[T]
  def sorted[TT >: T](implicit ordered: Ordering[TT], arg0: ClassTag[TT]): Dataset[T]
  def size: Long
  def append[TT >: T](ds: Dataset[TT])(implicit arg0: ClassTag[TT]): Dataset[TT]
}

trait PartitionMapper[-T, U] {
  def mapPartition(iter: Iterator[T], append: U => Unit): Unit
}
