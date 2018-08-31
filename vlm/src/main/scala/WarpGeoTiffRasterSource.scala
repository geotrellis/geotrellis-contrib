package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import cats.effect.IO

import java.net.URI
import java.nio.file.Paths

import geotrellis.util.{FileRangeReader, RangeReader, StreamingByteReader}

case class WarpGeoTiffRasterSource(
  uri: String,
  crs: CRS,
  resampleMethod: ResampleMethod = NearestNeighbor
) extends RasterSource {
  // FIX: read and reproject tiff segments using RegionReproject to avoid oversampling past bounds

  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  private val transform = Transform(tiff.crs, crs)
  private val backTransform = Transform(crs, tiff.crs)
  override lazy val rasterExtent = ReprojectRasterExtent(tiff.rasterExtent, transform)

  def extent: Extent = rasterExtent.extent
  def cols: Int = rasterExtent.cols
  def rows: Int = rasterExtent.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val intersectingWindows: Map[GridBounds, RasterExtent] =
      windows.flatMap { case targetRasterExtent: RasterExtent =>
        val sourceExtent: Extent = ReprojectRasterExtent(targetRasterExtent, backTransform).extent

        // sourceExtent may be covering pixels that we won't actually need. Tragic, but we only read in squares.
        if (sourceExtent.intersects(tiff.extent)) {
          val sourceGridBounds: GridBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
          // sourceGridBounds will contribute to this region
          Some((sourceGridBounds, targetRasterExtent))
        } else None
      }.toMap

    tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
      val targetRasterExtent = intersectingWindows(gb)
      val sourceRaster = Raster(tile, tiff.rasterExtent.extentFor(gb))
      // sourceRaster.reproject(targetRasterExtent, transform, backTransform, resampleMethod)
      val rr = implicitly[RasterRegionReproject[MultibandTile]]
      rr.regionReproject(sourceRaster, tiff.crs, crs, targetRasterExtent, targetRasterExtent.extent.toPolygon, resampleMethod)
    }
  }

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod): RasterSource = ???
}
