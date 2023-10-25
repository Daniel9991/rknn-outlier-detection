package rknn_outlier_detection.kdtrey5.coordinates

/** A coordinate system defines:
 *    1) points within a multi-dimensional space,
 *    2) a means to compute distance between those points, and
 *    3) a means to compare and order distances
 *    4) a means to compute whether a given point falls within two other ordered points
 */
trait CoordinateSystem {

  /** A point in multi-dimensional space. */
  trait Point {

    /** Returns the distance between this point and another point */
    def |-|(other: POINT): DISTANCE
  }

  /** Distance between two points or a point and a plane */
  trait Distance extends Ordered[DISTANCE]

  // corresponding virtual types needing to be set by sub-traits
  type POINT <: Point
  type DISTANCE <: Distance

  /** Returns true if a `target` point potentially falls between the range of ordered points `p1` and `p2` or within
   *  `distance` range of either of them.
   */
  def within(target: POINT, p1: POINT, p2: POINT, distance: DISTANCE): Boolean
}
