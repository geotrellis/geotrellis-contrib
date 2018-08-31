package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, RasterRegionReproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.proj4._
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

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): GeoTiffRasterSource =
    new GeoTiffRasterSource(uri) {
      @transient private lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

      private val baseCols: Int = tiff.cols
      private val baseRows: Int = tiff.rows
      private val baseCRS: CRS = tiff.crs
      private val baseExtent: Extent = tiff.extent
      private val baseRasterExtent: RasterExtent = RasterExtent(baseExtent, baseCols, baseRows)

      override def bandCount: Int = tiff.bandCount
      override def cellType: CellType = tiff.cellType

      override def crs: CRS = targetCRS

      private val transform: Transform = Transform(baseCRS, targetCRS)
      private val backTransform: Transform = Transform(targetCRS, baseCRS)

      override lazy val rasterExtent: RasterExtent = ReprojectRasterExtent(baseRasterExtent, transform)
      override def extent: Extent = rasterExtent.extent

      override def cols: Int = rasterExtent.cols
      override def rows: Int = rasterExtent.rows

      override def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
        val intersectingWindows: Map[GridBounds, RasterExtent] =
          windows.map { case targetRasterExtent =>
            val sourceExtent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
            val sourceGridBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = false)

            (sourceGridBounds, targetRasterExtent)
          }.toMap

        tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
          val targetRasterExtent = intersectingWindows(gb)
          val sourceRaster = Raster(tile, baseRasterExtent.extentFor(gb, clamp = false))

          val rr = implicitly[RasterRegionReproject[MultibandTile]]
          rr.regionReproject(
            sourceRaster,
            baseCRS,
            targetCRS,
            targetRasterExtent,
            targetRasterExtent.extent.toPolygon,
            resampleMethod
          )
        }
      }
    }

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

object GeoTiffRasterSource {
  def apply(
    uri: String,
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): GeoTiffRasterSource =
    GeoTiffRasterSource(uri).withCRS(targetCRS, resampleMethod)
}
