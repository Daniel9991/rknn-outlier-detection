package rknn_outlier_detection.kdtrey5

import scala.collection._
import scala.reflect.ClassTag

import rknn_outlier_detection.kdtrey5.data._
import rknn_outlier_detection.kdtrey5.mapreduce._

/**
 * Builder for KDTree's.
 */
trait KDTreeBuilder {
    import KDTreeBuilder._

    /** Concrete implementations must implement the `newDataSet` factory method.
     *
     *  This method is left abstract to support pluging-in different Dataset backends (e.g. Spark or otherwise)
     */
    def newDataset[T](): Dataset[T]

    /** Build a new KDTree using the supplied key-value dataset, with tree pages having up to `fanout` keys
     *  and branch nodes (or values).
     *
     *  The algorithm works as follows,
     *    1) sort the dataset by key, assumed to be ~O(n * log(n))
     *    2) iterate through dataset,
     *         a) emit bottom leaves, with one leaf per `fanout` keys/values.
     *         b) for each leaf, emit a node entry to be added to the parent branch.
     *    3) (recursion entrypoint)
     *       iterate through emitted node entries,
     *         a) emit a new branch node for each group of `fanout` entries,
     *         b) for each new branch node, emit a node entry to be added to the parent branch
     *    4) if there have been more than one emitted node, go back to #3
     *       if not, it means we have generated the root branch ("trunk") of the tree
     *
     *   A few noteworthy implementation notes:
     *
     *   *  The algorithm is iterative/recursive and therefore benefits from caching emitted data
     *      (i.e. on a framework such as Spark).
     *
     *   *  Iteration through the dataset is actually parallelized through the `mapPartition`
     *      primitive (once again, available on Spark).   This parallelization trades off
     *      optimal tree construction (perfectly balanced tree with minimum number of nodes) although
     *      the impact should be minimal if the size of the dataset is much larger (read: several
     *      orders of magnitude) than the level of parallelism.
     *
     *   Overall the algorithm should be rather efficient on a scale-out architecture.
     */
    def build[K, V](
        input: Dataset[(K, V)],
        fanout: Int
    )(implicit ordering: Ordering[K],
     ktag: ClassTag[K],
     vtag: ClassTag[V]
    ): KDTreeData[K, V] = {
        type N = KDNode[K, V]
        var current = KVDataset(input).sortByKey
        var result = newDataset[KDNode[K, V]]()
        val nodesPerLevel = mutable.Buffer[Long]()
        var level = 0

        //debug("Generate leaves...")
        var branches = current.mapPartitions((partition: Int, partitions: Int) => {
            //debug(s"mapPartitions leaves partition=$partition")
            val idGenerator = new PartitionIdGenerator(partition, partitions)
            new PartitionMapper[(K, V), N] {
                override def mapPartition(iter: Iterator[(K, V)], append: N => Unit): Unit = {
                    var size = 0
                    var keys = new Array[K](fanout)
                    var values = new Array[V](fanout)
                    while (iter.hasNext) {
                        if (size >= fanout) {
                            // emit full leaf
                            val id = s"Leaf#${partition}-${idGenerator.next()}"
                            append(KDLeaf(id, keys, values, size))
                            //debug(s"Append: ${KDLeaf(id, keys, values, size)}")

                            // reset accumulators
                            size = 0
                            keys = new Array[K](fanout)
                            values = new Array[V](fanout)
                        }

                        val (key, value) = iter.next()
                        keys(size) = key
                        values(size) = value
                        size += 1
                    }
                    if (size > 0) {
                        // emit partially filled leaf
                        val id = s"Leaf#${partition}-${idGenerator.next()}"
                        append(KDLeaf(id, keys, values, size))
                        //debug(s"Append: ${KDLeaf(id, keys, values, size)}")
                    }
                }
            }
        })
        nodesPerLevel += branches.count()
        result = result.append(branches)
        level += 1

        //debug(s"Generate branches level $level ...")
        while (branches.size > 1) {
            branches = branches.mapPartitions((partition: Int, partitions: Int) => {
                //debug(s"mapPartitions branches level=$level partition=$partition")
                val idGenerator = new PartitionIdGenerator(partition, partitions)
                new PartitionMapper[N, KDBranch[K, V]] {
                    override def mapPartition(iter: Iterator[N], append: KDBranch[K, V] => Unit): Unit = {
                        var size = 0
                        var keys = new Array[K](fanout)
                        var nodes = new Array[NodeId](fanout)
                        var lastKey: K = null.asInstanceOf[K]
                        while (iter.hasNext) {
                            val node = iter.next()
                            keys(size) = node.keys(0)
                            lastKey = node.lastKey
                            nodes(size) = node.id
                            size += 1

                            if (size >= fanout) {
                                // emit full branch
                                val id = s"Branch#${level}-${partition}-${idGenerator.next()}"
                                append(KDBranch(id, keys, lastKey, nodes, size))
                                //debug(s"Append: ${KDBranch(id, keys, nodes, size)}")

                                // reset accumulators
                                size = 0
                                keys = new Array[K](fanout)
                                nodes = new Array[NodeId](fanout)
                            }
                        }
                        if (size > 0) {
                            // emit partially filled branch
                            val id = s"Branch#${level}-${partition}-${idGenerator.next()}"
                            append(KDBranch(id, keys, lastKey, nodes, size))
                            //debug(s"Append: ${KDBranch(id, keys, nodes, size)}")
                        }
                    }
                }
            })
            //debug(s"After: branches=$branches")
            nodesPerLevel += branches.count()
            result = result.append(branches)
            level += 1
        }
        val root = branches.toSeq.head
        KDTreeData(root.id, result, nodesPerLevel)
    }
}

object KDTreeBuilder {

    /** Return data type for KDTreeBuilder */
    case class KDTreeData[K, V](
                                   rootId: NodeId,
                                   nodes: Dataset[KDNode[K, V]],
                                   nodesPerLevel: Seq[Long]) {

        /** Store the KDTree data into a Key-Value store */
        def store(store: KVStore[K, V]): Unit = {
            for (node <- nodes.toSeq) {
                store.store(node.id, node)
            }
            store.rootId = rootId
        }
    }

    private[kdtrey5] class PartitionIdGenerator(val partition: Int, val partitions: Int) {
        var _id: Long = partition

        def next(): Long = {
            val next = _id
            _id += partitions
            next
        }
    }

    /* debug facilities commentted out for performance but left intact to facilitate eventual
       debugging (or understanding of the algorithm for the curious) */

    /* uncomment this if needed
    private def debug(s: String) = {
      println(s)
    }
   */

}
