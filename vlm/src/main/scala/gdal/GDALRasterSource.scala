package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.vector._

import org.gdal.osr.SpatialReference


case class GDALRasterSource(uri: String) extends RasterSource {
  @transient private lazy val dataset = GDAL.open(uri)
  private lazy val data = GDALSourceData(dataset)

  lazy val cols: Int = data.cols
  lazy val rows: Int = data.rows

  lazy val extent = data.extent

  lazy val bandCount: Int = data.bandCount

  lazy val crs: CRS = data.crs

  lazy val cellType: CellType = data.cellType

  lazy val noDataValue: Option[Double] = data.noDataValue

  private lazy val reader = GDALReader(dataset, bandCount, noDataValue)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    read(rasterExtent.gridBoundsFor(extent, clamp = false), bands)

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val targetExtent = rasterExtent.rasterExtentFor(bounds).extent
    val actualBounds = rasterExtent.gridBoundsFor(targetExtent, clamp = true)

    val initialTile = reader.read(actualBounds, bands)

    val tile =
      if (initialTile.cols != bounds.width || initialTile.rows != bounds.height) {
        val tiles =
          initialTile.bands.map { band =>
            val protoTile = band.prototype(bounds.width, bounds.height)

            protoTile.update(bounds.colMin - actualBounds.colMin, bounds.rowMin - actualBounds.rowMin, band)
            protoTile
          }

        MultibandTile(tiles)
      } else
        initialTile

    Some(Raster(tile, rasterExtent.extentFor(bounds)))
  }

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, data, None, resampleMethod)

  def reproject(targetCRS: CRS, resampleMethod: ResampleMethod, rasterExtent: RasterExtent): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, data, Some(rasterExtent))
}
