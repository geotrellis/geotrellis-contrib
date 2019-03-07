package geotrellis.contrib.polygonal

import cats.Monoid
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.util.{GetComponent, MethodExtensions}
import spire.syntax.cfor._

object PolygonalSummary {
  final val DefaultOptions = Rasterizer.Options(includePartial = true, sampleType = PixelIsArea)

  def apply[A, R](
    raster: A,
    geometry: Geometry,
    acc: R,
    options: Rasterizer.Options
  )(implicit
    cellRegister: CellVisitor[A, R],
    getRasterExtent: GetComponent[A, RasterExtent]
  ): R = {
    val rasterExtent: RasterExtent = getRasterExtent.get(raster)
    val rasterArea: Polygon = rasterExtent.extent.toPolygon
    var result = acc

    geometry match {
      case area: TwoDimensions if (rasterArea.coveredBy(area)) =>
        cfor(0)(_ < rasterExtent.cols, _ + 1) { col =>
          cfor(0)(_ < rasterExtent.rows, _ + 1) { row =>
            result = cellRegister.register(raster, col, row, result)
          }
        }

      case _ =>
        Rasterizer.foreachCellByGeometry(geometry, rasterExtent, options)  { (col: Int, row: Int) =>
          result = cellRegister.register(raster, col, row, result)
        }
    }

    result
  }

  trait PolygonalSummaryMethods[A] extends MethodExtensions[A] {

    def polygonalSummary[R](
      geometry: Geometry,
      emptyResult: R,
      options: Rasterizer.Options
    )(implicit
      ev0: CellVisitor[A, R],
      ev1: GetComponent[A, RasterExtent]
    ): R = PolygonalSummary(self, geometry, emptyResult, options)

    def polygonalSummary[R](
      geometry: Geometry,
      emptyResult: R
    )(implicit
      ev0: CellVisitor[A, R],
      ev1: GetComponent[A, RasterExtent]
    ): R = PolygonalSummary(self, geometry, emptyResult, PolygonalSummary.DefaultOptions)

    def polygonalSummary[R](
      geometry: Geometry,
      options: Rasterizer.Options
    )(implicit
      ev0: CellVisitor[A, R],
      ev1: GetComponent[A, RasterExtent],
      ev2: Monoid[R]
    ): R = PolygonalSummary(self, geometry, Monoid[R].empty, options)

    def polygonalSummary[R](
      geometry: Geometry
    )(implicit
      ev0: CellVisitor[A, R],
      ev1: GetComponent[A, RasterExtent],
      ev2: Monoid[R]
    ): R = PolygonalSummary(self, geometry, Monoid[R].empty, PolygonalSummary.DefaultOptions)
  }

  trait ToPolygonalSummaryMethods {
    implicit class withPolygonalSummaryMethods[A](val self: A) extends PolygonalSummaryMethods[A]
  }

  object ops extends ToPolygonalSummaryMethods
}