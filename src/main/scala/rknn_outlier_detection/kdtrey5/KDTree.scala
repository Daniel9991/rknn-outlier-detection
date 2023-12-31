package rknn_outlier_detection.kdtrey5

import coordinates._
import rknn_outlier_detection.kdtrey5.data._
import skiis2.Skiis

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

case class RangeFindStats(branchesRetrieved: Int, leavesRetrieved: Int)

trait RangeFindResult[K, V] {
    val values: Skiis[(K, V)]
    def stats: RangeFindStats
}

trait KNeighborsSearchResult[K, V] {
  val values: Seq[(K, V)]
  def stats: RangeFindStats
}

/**
 * A KD-Tree is a tree-like data structure that supports efficient range queries over multi-dimensional
 * key space.
 *
 * Similarly to B+Trees, this implementation supports n-ary nodes (aka pages) for efficient retrieval
 * over high-latency storage.
 */
trait KDTree {
    type COORDS <: CoordinateSystem

    /** Implementations must supply a coordinate system that defines notions of POINTs in a multi-dimensional space
      and DISTANCE between such POINTs. */
    val coords: COORDS

    type Point = coords.POINT
    type Distance = coords.DISTANCE

    /** Keys of the tree are POINTs, values are left fully abstract */
    type K = Point
    type V

    /** Nodes are defined in terms of the above key and value types */
    type Node = KDNode[K, V]
    type Branch = KDBranch[K, V]
    type Leaf = KDLeaf[K, V]

    /** A backing store is needed to retrieve keys and values */
    val store: KVStore[K, V]

    /** Find all existing points within `distance` of the given `point */
    def rangeFind(
         target: Point,
         distance: Distance
     )(implicit skiisContext: Skiis.Context
     ): RangeFindResult[K, V] = {
        val branchesRetrieved = new AtomicInteger(0)
        val leavesRetrieved = new AtomicInteger(0)

        def updateStats(node: Node) = node match {
            case b: Branch => branchesRetrieved.incrementAndGet()
            case l: Leaf   => leavesRetrieved.incrementAndGet()
        }

        // backpressure happens through queue created in `parMapWithQueue`
        val nodeQueue = new Skiis.Queue[Node](Int.MaxValue, maxAwaiting = skiisContext.parallelism)

        // start at the root of the tree
        val root = store.load(store.rootId)
        updateStats(root)
        nodeQueue += root

        /** implements a depth-first range search */
        val results = nodeQueue.parMapWithQueue[(Point, V)]((node, values) => {
            try {
                //debug(s"findNext() target=$target distance=$distance")
                //debug(s"findNext() current node ${node.id}")
                node match {
                    case branch: KDBranch[K, V] =>
                        // evaluate all branches to see if they contain values that are possibly
                        // within the range, if so push the child node (branch or leaf) on the stack

                        var pos = 0
                        while (pos < branch.keys.length && branch.keys(pos) != null) {
                            val p_current = branch.keys(pos)
                            val hasFollowing = (pos < branch.keys.length - 1 && branch.keys(pos + 1) != null)
                            val p_next = if (hasFollowing) branch.keys(pos + 1) else branch.lastKey
                            //debug(s"findNext() p_current $p_current")

                            if (coords.within(target, p_current, p_next, distance)) {
                                val child = store.load(branch.nodes(pos))
                                //debug(s"enqueue: ${child}")
                                updateStats(child)
                                nodeQueue += child
                            }

                            pos += 1
                        }

                    case leaf: KDLeaf[K, V] =>
                        //debug(s"findNext() leaf ${leaf.id}")
                        var pos = 0
                        while (pos < leaf.keys.length && leaf.keys(pos) != null) {
                            val p_current = leaf.keys(pos)
                            if ((target |-| p_current) <= distance) {
                                //debug(s"findNext() leaf push ${(p_current, leaf.values(pos))}")
                                values += (p_current, leaf.values(pos))
                            }
                            pos += 1
                        }
                } // match
            } catch {
                case e: Exception => e.printStackTrace(); throw e
            }
        })(skiisContext)
        new RangeFindResult[K, V] {
            override val values = results
            override def stats = RangeFindStats(branchesRetrieved.get, leavesRetrieved.get)
        }
    }

  /** Find k existing points closest to the given `point` */
  def findKNeighbors(
    target: Point,
    k: Int,
  )(implicit skiisContext: Skiis.Context
  ): KNeighborsSearchResult[K, V] = {
    val branchesRetrieved = new AtomicInteger(0)
    val leavesRetrieved = new AtomicInteger(0)

    def updateStats(node: Node) = node match {
      case b: Branch => branchesRetrieved.incrementAndGet()
      case l: Leaf => leavesRetrieved.incrementAndGet()
    }

    // backpressure happens through queue created in `parMapWithQueue`
    val nodeQueue = new Skiis.Queue[Node](Int.MaxValue, maxAwaiting = skiisContext.parallelism)

    // start at the root of the tree
    val root = store.load(store.rootId)
    updateStats(root)
    nodeQueue += root

    val branchDistances = new ArrayBuffer[Distance]()
    val leafDistances = new ArrayBuffer[Distance]()

    /** implements a depth-first neighbors search */
    val results = nodeQueue.parMapWithQueue[(Point, V)]((node, values) => {
      try {
        //debug(s"findNext() target=$target distance=$distance")
        debug(s"findNext() current node ${node.id}")
        node match {
          case branch: KDBranch[K, V] =>
            // evaluate all branches to see if they contain values that are possibly
            // within the range, if so push the child node (branch or leaf) on the stack

            var pos = 0
            while (pos < branch.keys.length && branch.keys(pos) != null) {
              val p_current = branch.keys(pos)
              val hasFollowing = (pos < branch.keys.length - 1 && branch.keys(pos + 1) != null)
              val p_next = if (hasFollowing) branch.keys(pos + 1) else branch.lastKey
              debug(s"findNext() p_current $p_current")

              val currentDistance = target |-| p_current

              // TODO take a look at coords.within
              // Can I use coords.within for this kind of search (does it make sense)?
              if (branchDistances.length == k && !(currentDistance <= branchDistances.last)) { /* do nothing */ }
              else {
                if (branchDistances.length < k) {
                  branchDistances.addOne(currentDistance)
                  branchDistances.sortInPlaceWith((d1, d2) => d1 < d2)
                }
                else if (currentDistance == branchDistances.last){
                  branchDistances(k-1) = currentDistance
                  branchDistances.sortInPlaceWith((d1, d2) => d1 < d2)
                }
                val child = store.load(branch.nodes(pos))
                updateStats(child)
                nodeQueue += child
              }

              pos += 1
            }

          case leaf: KDLeaf[K, V] =>
            debug(s"findNext() leaf ${leaf.id}")

            var pos = 0
            while (pos < leaf.keys.length && leaf.keys(pos) != null) {

              val p_current = leaf.keys(pos)
              val currentDistance = target |-| p_current

              // TODO find a way to omit same node
              if (leafDistances.length == k && !(currentDistance < leafDistances.last)) { /* do nothing */ }
              else{
                if (leafDistances.length < k)
                  leafDistances.addOne(currentDistance)
                else {
                  leafDistances(k-1) = currentDistance
                }
                leafDistances.sortInPlaceWith((d1, d2) => d1 < d2)
                debug(s"findNext() leaf push ${(p_current, leaf.values(pos))}")
                values += (p_current, leaf.values(pos))
              }
              pos += 1
            }
        } // match

      } catch {
        case e: Exception => e.printStackTrace(); throw e
      }
    })(skiisContext)

    var finalResults: Seq[(Point, V)] = null
    if(results.size > k)
      finalResults = results.take(results.size).sortWith((t1, t2) => (target |-| t1._1) < (target |-| t2._1)).slice(0, k)
    else
      finalResults = results.take(results.size)

    println(s"branches retrieved ${branchesRetrieved.get()}")
    println(s"leaves retrieved ${leavesRetrieved.get()}")

    new KNeighborsSearchResult[K, V] {
      override val values: Seq[(K, V)] = finalResults
      override def stats = RangeFindStats(branchesRetrieved.get, leavesRetrieved.get)
    }
  }
    /* debug facilities commentted out for performance but left intact to facilitate eventual
       debugging (or understanding of the algorithm for the curious) */

    private def debug(s: String) = {
      println(s)
    }
}

/** A KD-Tree using a bitset-based coordinate system */
trait BitsetKDTree extends KDTree {
    override type COORDS = BitsetCoordinateSystem.type
    override val coords = BitsetCoordinateSystem
}

trait VectorKDTree extends KDTree {
    override type COORDS = VectorCoordinateSystem.type
    override val coords = VectorCoordinateSystem
}
