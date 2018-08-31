package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, RasterRegionReproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader


case class GeoTiffRasterSource(uri: String) extends RasterSource {
  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def extent: Extent = tiff.extent
  def crs: CRS = tiff.crs
  def cols: Int = tiff.tile.cols
  def rows: Int = tiff.tile.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): WarpGeoTiffRasterSource =
    WarpGeoTiffRasterSource(uri, targetCRS, resampleMethod)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val intersectionWindows: Traversable[GridBounds] =
      windows.map { case targetRasterExtent =>
        rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = false)
      }

    tiff.crop(intersectionWindows.toSeq).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
  }
}
