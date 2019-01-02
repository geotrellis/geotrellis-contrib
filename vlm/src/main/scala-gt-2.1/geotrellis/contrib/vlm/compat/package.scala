package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.proj4._

/**
 *
 * Cross compatibility shims for GeoTrellis 2.1.x.
 */
package object compat {
  implicit class WithReprojectExtentAsPolygon(val e: Extent) extends AnyVal {

    // Copied from geotrellis/vector/reproject/Reproject.scala
    /** Performs adaptive refinement to produce a Polygon representation of the projected region.
     *
     * Generally, rectangular regions become curvilinear regions after geodetic projection.
     * This function creates a polygon giving an accurate representation of the post-projection
     * region.  This function does its work by recursively splitting extent edges, until a relative
     * error criterion is met.  That is,for an edge in the source CRS, endpoints, a and b, generate
     * the midpoint, m.  These are mapped to the destination CRS as a', b', and c'.  If the
     * distance from m' to the line a'-b' is greater than relErr * distance(a', b'), then m' is
     * included in the refined polygon.  The process recurses on (a, m) and (m, b) until
     * termination.
     *
     * @param transform
     * @param relError   A tolerance value telling how much deflection is allowed in terms of
     *                   distance from the original line to the new point
     */
    def reprojectExtentAsPolygon(extent: Extent, transform: Transform, relError: Double): Polygon = {
      import math.{abs, pow, sqrt}

      def refine(p0: (Point, (Double, Double)), p1: (Point, (Double, Double))): List[(Point, (Double, Double))] = {
        val ((a, (x0, y0)), (b, (x1, y1))) = (p0, p1)
        val m = Point(0.5 * (a.x + b.x), 0.5 * (a.y + b.y))
        val (x2, y2) = transform(m.x, m.y)

        val deflect = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1) / sqrt(pow(y2 - y1, 2) + pow(x2 - x1, 2))
        val length = sqrt(pow(x0 - x1, 2) + pow(y0 - y1, 2))

        val p2 = m -> (x2, y2)
        if (deflect / length < relError) {
          List(p2)
        } else {
          refine(p0, p2) ++ (p2 :: refine(p2, p1))
        }
      }

      val pts = Array(extent.southWest, extent.southEast, extent.northEast, extent.northWest)
        .map{ p => (p, transform(p.x, p.y)) }

      Polygon ( ((pts(0) :: refine(pts(0), pts(1))) ++
        (pts(1) :: refine(pts(1), pts(2))) ++
        (pts(2) :: refine(pts(2), pts(3))) ++
        (pts(3) :: refine(pts(3), pts(0))) ++
        List(pts(0))).map{ case (_, (x, y)) => Point(x, y) } )
    }

    def reprojectAsPolygon(src: CRS, dest: CRS, relErr: Double): Polygon = e.reprojectAsPolygon(Transform(src, dest), relErr)
    def reprojectAsPolygon(transform: Transform, relErr: Double): Polygon = reprojectExtentAsPolygon(e, transform, relErr)
  }
}
