package geotrellis.contrib.polygonal

import cats.Monoid
import geotrellis.vector._
import simulacrum._

@typeclass trait PolygonalSummary[T] {
  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, geometry: G): Feature[G, R] =
    polygonalSummary(self, Feature(geometry, Monoid[R].empty))

  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, feature: Feature[G, R]): Feature[G, R]
}