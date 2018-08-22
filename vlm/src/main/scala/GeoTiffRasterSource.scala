package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, Reproject}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import cats.effect.IO

import java.net.URI
import java.nio.file.Paths

case class GeoTiffRasterSource(uri: String) extends RasterSource {
  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def extent: Extent = tiff.extent
  def crs: CRS = tiff.crs
  def cols: Int = tiff.tile.cols
  def rows: Int = tiff.tile.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val intersectionWindows: Traversable[GridBounds] =
      windows.map { case targetRasterExtent =>
        rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
      }

    tiff.crop(intersectionWindows.toSeq).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb))
    }
  }
}
