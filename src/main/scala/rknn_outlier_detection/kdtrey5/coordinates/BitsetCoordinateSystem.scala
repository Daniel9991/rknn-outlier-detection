package rknn_outlier_detection.kdtrey5.coordinates

import java.util.BitSet

/**
 * An arbitrary-length binary (biset-based) coordinate system.
 *
 * e.g. points can be expressed as `011011101`, `10111011`, etc.
 *
 * Distance is defined as the Hamming Distance (https://en.wikipedia.org/wiki/Hamming_distance) between two points.
 */
object BitsetCoordinateSystem extends CoordinateSystem {
  type DISTANCE = BitsetDistance
  type POINT = BitsetPoint

  case class BitsetDistance(value: Long) extends super.Distance {
    override def compare(that: DISTANCE): Int = {
      if (this.value > that.value) 1
      else if (this.value == that.value) 0
      else -1
    }
  }

  implicit val bitsetOrdering = new Ordering[POINT] {
    override def compare(x: BitsetPoint, y: BitsetPoint): Int = {
      if (x.length != y.length) throw new Exception("Cannot compare points of different lengths")
      var pos = x.length - 1
      while (pos >= 0) {
        val xbit = x.bits.get(pos)
        val ybit = y.bits.get(pos)
        if (xbit && !ybit) return 1
        if (!xbit && ybit) return -1
        pos -= 1
      }
      return 0
    }
  }

  case class BitsetPoint(val bits: BitSet, val length: Int) extends super.Point {
    override def toString = {
      s"BitsetPoint(${asBinaryString})"
    }

    override def |-|(other: POINT): DISTANCE = {
      val xored = BitsetCoordinateSystem.clone(this.bits)
      xored.xor(other.bits)
      BitsetDistance(xored.cardinality())
    }

    def asBinaryString: String = {
      val sb = new StringBuilder()
      var pos = length - 1
      while (pos >= 0) {
        val ch = if (bits.get(pos)) '1' else '0'
        sb.append(ch)
        pos -= 1
      }
      sb.toString
    }

    def asByteArray: Array[Byte] = {
      bits.toByteArray
    }
  }

  /** Create a BitsetPoint from a string, e.g. "01100111" */
  def pointFromBinaryString(bits: String): BitsetPoint = {
    BitsetPoint(bitSetFromString(bits), bits.length)
  }

  /** Create a BitsetPoint from a `BitSet` */
  def pointFromBitSet(bits: BitSet, len: Int): BitsetPoint = {
    BitsetPoint(bits, len)
  }

  /** Create a BitsetPoint from a byte array */
  def pointFromByteArray(bytes: Array[Byte], bits: Int): BitsetPoint = {
    BitsetPoint(BitSet.valueOf(bytes), bits)
  }

  private[BitsetCoordinateSystem] def clone(bs: BitSet): BitSet = {
    // BitSet in Java forces use of mutation so need to clone often ...
    bs.clone.asInstanceOf[BitSet]
  }

  // note: unoptimized
  def bitSetFromString(binary: String): BitSet = {
    val bs = new BitSet()
    binary.reverse.zipWithIndex foreach {
      case (bit, pos) =>
        bit match {
          case '0' => // false by default
          case '1' => bs.set(pos, true)
          case _   => throw new Exception(s"illegal value for bits: $binary")
        }
    }
    bs
  }

  override def within(target: POINT, p1: POINT, p2: POINT, distance: DISTANCE): Boolean = {
    var pos = target.length - 1
    var d = 0
    while ({
      val tb = target.bits.get(pos)
      val p1b = p1.bits.get(pos)
      val p2b = p2.bits.get(pos)
      if (p1b == p2b) {
        if (tb != p1b) {
          d += 1
          if (d > distance.value) {
            return false
          }
        }
      } else {
        return (d <= distance.value)
      }
      pos -= 1
      (pos >= 0)
    }) {}
    return (d <= distance.value)
  }
}
