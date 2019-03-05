package geotrellis.contrib.polygonal

import cats.Monoid
import geotrellis.vector._
import simulacrum._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster._

@typeclass trait PolygonalSummary[T] {

  /** Polygonal summary over for pixels under geomery.
   * Monoid.empty will be used to get the initial value of the accumulator.
   * Rasterizer.Options are to consider pixels as areas and to include partial intersections.
   *
   * @param self Raster-like type containing pixel data
   * @param feature Geometry for area of summery
   * @param options Rasterizer options determine which pixels are judged to be intersecting the geometry.
   */
  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, geometry: G): Feature[G, R] =
    polygonalSummary(self, geometry, Rasterizer.Options(includePartial =  true, sampleType = PixelIsArea))

  /** Polygonal summary over for pixels under geomery.
   * Monoid.empty will be used to get the initial value of the accumulator.
   *
   * @param self Raster-like type containing pixel data
   * @param feature Geometry for area of summery
   * @param options Rasterizer options determine which pixels are judged to be intersecting the geometry.
   */
  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, geometry: G, options: Rasterizer.Options): Feature[G, R] =
    polygonalSummary(self, Feature(geometry, Monoid[R].empty), options)

  /** Polygonal summary over for pixels under geomery.
   * The feature data value will be used as a starting point of the accumulator.
   * Rasterizer.Options are to consider pixels as areas and to include partial intersections.
   *
   * @param self Raster-like type containing pixel data
   * @param feature Geometry for area of summery
   */
  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, feature: Feature[G, R]): Feature[G, R] =
    polygonalSummary(self, feature, Rasterizer.Options(includePartial =  true, sampleType = PixelIsArea))

  /** Polygonal summary over for pixels under geomery.
   * The feature data value will be used as a starting point of the accumulator.
   *
   * @param self Raster-like type containing pixel data
   * @param feature Geometry for area of summery
   * @param options Rasterizer options determine which pixels are judged to be intersecting the geometry.
   */
  def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](self: T, feature: Feature[G, R], options: Rasterizer.Options): Feature[G, R]
}