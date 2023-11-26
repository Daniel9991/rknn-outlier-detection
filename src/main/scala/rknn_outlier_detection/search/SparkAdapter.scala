package rknn_outlier_detection.search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rknn_outlier_detection.kdtrey5.mapreduce.{Dataset, PartitionMapper}

import scala.collection.mutable
import scala.reflect.ClassTag

class SparkDataset[T](rdd: RDD[T], sc: SparkContext) extends Dataset[T]{

    override def cache(): Unit = {
        rdd.cache()
    }

    override def filter(f: T => Boolean): Dataset[T] = {
        new SparkDataset(rdd.filter(f), sc)
    }

    override def map[U](f: T => U)(implicit arg0: ClassTag[U]): Dataset[U] = {
        new SparkDataset[U](rdd.map(f), sc)
    }

    override def flatmap[U](f: T => Iterator[U])(implicit arg0: ClassTag[U]): Dataset[U] = {
        new SparkDataset(rdd.flatMap(f), sc)
    }

    override def mapPartitions[TT >: T, U](f: (Int, Int) => PartitionMapper[TT, U])(implicit arg0: ClassTag[U]): Dataset[U] = {
        mapPartition(f(0, 1))
    }

    override def mapPartition[U](f: PartitionMapper[T, U])(implicit arg0: ClassTag[U]): Dataset[U] = {

        val resultingRDD = rdd.mapPartitions(iterator => {
            iterator.map(element => {
                var newValue: U = null.asInstanceOf[U]
                f.mapPartition(Iterator(element), mappedValue => {newValue = mappedValue})
                newValue
            })
        })

        new SparkDataset(resultingRDD, sc)
    }

    override def toSeq: collection.Seq[T] = {
        rdd.collect().toSeq
    }

    override def sorted[TT >: T](implicit ordered: Ordering[TT], arg0: ClassTag[TT]): Dataset[T] = {
        new SparkDataset(rdd.sortBy(x => x.asInstanceOf[TT]), sc)
    }

    override def size: Long = {
        rdd.count()
    }

    override def append[TT >: T](ds: Dataset[TT])(implicit arg0: ClassTag[TT]): Dataset[TT] = {
        val appended = rdd.collect().concat(ds.toSeq).toSeq
        new SparkDataset[TT](sc.parallelize(appended), sc)
    }
}
