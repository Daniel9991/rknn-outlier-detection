package rknn_outlier_detection.kdtrey5.coordinates

/**
 * An arbitrary-length vector coordinate system.
 *
 * e.g. points can be Array(1,2), Array(1,2,3), ...
 *
 * Distance is define as dot-product between two points.
 */
object VectorCoordinateSystem extends CoordinateSystem {
  type DISTANCE = VectorDistance
  type POINT = VectorPoint

  case class VectorDistance(value: Float) extends super.Distance {
    override def compare(that: DISTANCE): Int = {
      if (this.value > that.value) 1
      else if (this.value == that.value) 0
      else -1
    }
  }

  implicit val vectorOrdering = new Ordering[POINT] {
    override def compare(x: VectorPoint, y: VectorPoint): Int = {
      val a1 = x.values
      val a2 = y.values
      if (a1.length != a2.length) throw new Exception("Cannot compare points of different lengths")
      var pos = 0
      while (pos < a1.length) {
        val v1 = a1(pos)
        val v2 = a2(pos)
        if (v1 > v2) return 1
        if (v1 < v2) return -1
        pos += 1
      }
      return 0
    }
  }

  case class VectorPoint(val values: Array[Long]) extends super.Point {
    override def toString = {
      s"VectorPoint(${values.toSeq.mkString(",")})"
    }

    override def |-|(other: POINT): DISTANCE = {
      if (this.values.length != other.values.length)
        throw new Exception(s"Can't compare: $this $other")
      var i = 0
      var sumOfSquares = 0.0f
      while (i < this.values.length) {
        val diff = this.values(i) - other.values(i)
        sumOfSquares += (diff * diff)
        i += 1
      }
      VectorDistance(Math.sqrt(sumOfSquares).toFloat)
    }
  }

  override def within(target: POINT, p1: POINT, p2: POINT, distance: DISTANCE): Boolean = {
    var pos = 0
    var sumOfSquares = 0L
    while ({
      val tv = target.values(pos)
      val p1v = p1.values(pos)
      val p2v = p2.values(pos)
      if (p1v == p2v) {
        val diff = tv - p1v
        sumOfSquares += (diff * diff)
        if (Math.sqrt(sumOfSquares).toFloat > distance.value) return false
      } else {
        val diff = Math.min(Math.abs(tv - p1v), Math.abs(tv - p2v))
        sumOfSquares += (diff * diff)
        return (Math.sqrt(sumOfSquares).toFloat <= distance.value)
      }
      pos += 1
      (pos < target.values.length)
    }) {}
    return (Math.sqrt(sumOfSquares).toFloat <= distance.value)
  }

  def dotProduct2(p1: POINT, p2: POINT): DISTANCE = {
    if (p1.values.length != p2.values.length) throw new Exception(s"Can't compare: $p1 $p2")
    // calculate dot-product
    var i = 0
    var distance = 0.0f
    while (i < p1.values.length) {
      distance += p1.values(i) * p2.values(i)
      i += 1
    }
    VectorDistance(distance)
  }

  private def dotProduct(a1: Array[Float], a2: Array[Float]): Float = {
    if (a1.length != a2.length) throw new Exception(s"Invalid lengths: $a1 $a2")
    var i = 0
    var distance = 0.0f
    while (i < a1.length) {
      distance += a1(i) * a2(i)
      i += 1
    }
    return distance
  }

}
