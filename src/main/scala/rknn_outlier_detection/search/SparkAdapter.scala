package rknn_outlier_detection.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.kdtrey5.mapreduce.{Dataset, PartitionMapper}

import scala.collection.mutable
import scala.reflect.ClassTag

class SparkAdapter[T](sparkDataset: RDD[T], sc: SparkContext) extends Dataset[T]{

    override def cache(): Unit = {
        sparkDataset.cache()
    }

    override def filter(f: T => Boolean): Dataset[T] = {
        new SparkAdapter(sparkDataset.filter(f), sc)
    }

    override def map[U](f: T => U)(implicit arg0: ClassTag[U]): Dataset[U] = {
        new SparkAdapter[U](sparkDataset.map(f), sc)
    }

    override def flatmap[U](f: T => Iterator[U])(implicit arg0: ClassTag[U]): Dataset[U] = {
        new SparkAdapter(sparkDataset.flatMap(f), sc)
    }

    override def mapPartitions[TT >: T, U](f: (Int, Int) => PartitionMapper[TT, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
        mapPartition(f(0, 1))
    }

    override def mapPartition[U](f: PartitionMapper[T, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
        new SparkAdapter({
            val buffer = new mutable.ArrayBuffer[U]()
            def append(u: U) = buffer.append(u)
            f.mapPartition(sparkDataset.toLocalIterator, append)
            sc.parallelize(buffer.toSeq)
        }, sc)
    }

    override def toSeq: collection.Seq[T] = {
        sparkDataset.collect().toSeq
    }

    override def sorted[TT >: T](implicit ordered: Ordering[TT], arg0: ClassTag[TT]): Dataset[T] = {
        new SparkAdapter(sparkDataset.sortBy(x => x.asInstanceOf[TT]), sc)
    }

    override def size: Long = {
        sparkDataset.count()
    }

    override def append[TT >: T](ds: Dataset[TT])(implicit arg0: ClassTag[TT]): Dataset[TT] = {
        val appended = sparkDataset.collect().concat(ds.toSeq).toSeq
        new SparkAdapter[TT](sc.parallelize(appended), sc)
    }
}
