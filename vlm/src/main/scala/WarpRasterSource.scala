package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, Reproject}
import geotrellis.vector._

import java.net.URI


class WarpRasterSource(
  val base: RasterSource,
  val crs: CRS,
  val options: Reproject.Options
) extends RasterSource {

  def uri: URI = base.uri
  def cols: Int = base.cols
  def rows: Int = base.rows
  def cellType: CellType = base.cellType
  def bandCount: Int = base.bandCount

  def extent: Extent = {
    val oldProjectedExtent = ProjectedExtent(base.extent, base.crs)

    oldProjectedExtent.reprojectAsPolygon(crs).envelope
  }

  private val transform = Transform(base.crs, crs)
  private val backTransform = Transform(crs, base.crs)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val rasterExtents: Traversable[RasterExtent] =
      windows.map { case targetRasterExtent =>
        val projectedExtent = ProjectedExtent(targetRasterExtent.extent, crs)

        val sourceExtent: Extent = projectedExtent.reprojectAsPolygon(base.crs).envelope

        RasterExtent(sourceExtent, targetRasterExtent.cols, targetRasterExtent.rows)
      }

    base.read(rasterExtents).map { raster =>
      raster.reproject(base.crs, crs, options)
    }
  }
}

object WarpRasterSource {
  def apply(
    rasterSource: RasterSource,
    crs: CRS,
    options: Reproject.Options
  ): WarpRasterSource =
    new WarpRasterSource(rasterSource, crs, options)

  def apply(rasterSource: RasterSource, crs: CRS): WarpRasterSource =
    apply(rasterSource, crs, Reproject.Options.DEFAULT)
}
