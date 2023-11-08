package rknn_outlier_detection.search

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import rknn_outlier_detection.kdtrey5.data.{InMemoryKVStore, KVStore}
import rknn_outlier_detection.kdtrey5.{KDTreeBuilder, VectorKDTree}
import rknn_outlier_detection.kdtrey5.mapreduce.Dataset

import scala.reflect.ClassTag
import java.util.{BitSet, Random}
import skiis2.Skiis

class SparkKDTreeTest extends AnyFunSuite {
    import rknn_outlier_detection.kdtrey5.coordinates.VectorCoordinateSystem._

    implicit val skiisContext: Skiis.Context = new Skiis.Context {
        override final val parallelism = 1
        override final val queue = 100 // must be > KDTree fanout
        override final val batch = 1
        override final val shutdownExecutor = true
        override final lazy val executor = Skiis.newFixedThreadPool("KDTreeTest", threads = 1)
    }

    private def debug(s: String) = {
        println(s)
    }

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Sparking2"))

    val builder = new KDTreeBuilder {
        override def newDataset[T](): Dataset[T] = new SparkAdapter(sc.parallelize(Seq.empty[T]), sc)
    }

    test("k-neighbors-search") {
        val points: Seq[(VectorPoint, String)] =
            for (x <- 1 to 5;
                 y <- 1 to 5) yield (VectorPoint(Array(x, y)), (x, y).toString)
        val kddata = builder.build(new SparkAdapter[(VectorPoint, String)](sc.parallelize(points), sc), fanout = 2)
        kddata.nodesPerLevel shouldBe Seq(13, 7, 4, 2, 1)

        val kdstore = new InMemoryKVStore[VectorPoint, String]()
        kddata.store(kdstore)
        val kdtree = new VectorKDTree {
            override type V = String
            override val store: KVStore[K, V] = kdstore // There is a KVStore available in the Spark package
        }
        val target = VectorPoint(Array(1, 2))
        val r2 = kdtree.findKNeighbors(target, 3)
        val results = r2.values.map(t => t._2)
        val expectedPoints = Seq("(1,1)", "(1,3)", "(2,2)")
        results.toSet shouldBe expectedPoints.toSet
        //    r2.stats.leavesRetrieved should be <= 7
        //    r2.stats.branchesRetrieved should be <= 8
    }
}

