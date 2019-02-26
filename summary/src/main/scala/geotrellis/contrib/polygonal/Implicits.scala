package geotrellis.contrib.polygonal

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import cats._

object Implicits {
  implicit val poloygonalSummaryForRasterTile: PolygonalSummary[Raster[Tile]] = new PolygonalSummary[Raster[Tile]] {
    // TODO: docstring
    def polygonalSummary[G <: Geometry, R: CellAccumulator: Monoid](
      self: Raster[Tile],
      feature: Feature[G, R]
    ): Feature[G, R] = {
      import CellAccumulator.ops._
      var result: R = feature.data
      val geom = feature.geom

      val options = Rasterizer.Options(includePartial =  true, sampleType = PixelIsArea)
      Rasterizer.foreachCellByGeometry(geom, self.rasterExtent, options)  { (col: Int, row: Int) =>
        val v = self.tile.getDouble(col, row)
        if (isData(v)) result = result.add(v)
      }

      Feature(geom, result)
    }
  }
}