package geotrellis.contrib.polygonal

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.util.GetComponent
import cats._

trait Implicits {
  implicit val rasterSinglebandHistogramVisitor = new CellVisitor[Raster[Tile], StreamingHistogram] {
    override def register(raster: Raster[Tile], col: Int, row: Int, acc: StreamingHistogram): StreamingHistogram = {
      val v = raster.tile.getDouble(col, row)
      acc.countItem(v, count = 1)
      acc
    }
  }

  implicit def rasterHasRasterExtent[T <: CellGrid[Int]] = new GetComponent[Raster[T], RasterExtent] {
    override def get: Raster[T] => RasterExtent = _.rasterExtent
  }
}

object Implicits extends Implicits