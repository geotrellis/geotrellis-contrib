package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, Reproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.vector._
import geotrellis.spark.tiling._

import java.net.URI


case class WarpRasterSource(
  base: RasterSource,
  crs: CRS,
  resampleMethod: ResampleMethod = NearestNeighbor
) extends RasterSource {

  def uri: URI = base.uri
  def cellType: CellType = base.cellType
  def bandCount: Int = base.bandCount

  private val options = Reproject.Options(method = resampleMethod)

  private val transform = Transform(base.crs, crs)
  private val backTransform = Transform(crs, base.crs)

  override def rasterExtent = ReprojectRasterExtent(base.rasterExtent, transform)
  def extent = rasterExtent.extent

  def cols: Int = rasterExtent.cols
  def rows: Int = rasterExtent.rows

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val rasterExtents: Traversable[RasterExtent] =
      windows.map { case targetRasterExtent =>
        val sourceExtent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
        val sourceGridBounds = base.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)

        RasterExtent(sourceExtent, sourceGridBounds.width, sourceGridBounds.height)
      }

    base.read(rasterExtents).map { raster =>
      raster.reproject(base.crs, crs, options)
    }
  }
}
