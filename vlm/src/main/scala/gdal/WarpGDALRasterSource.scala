package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.RasterSource

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import org.gdal.gdal._
import org.gdal.osr.SpatialReference


case class WarpGDALRasterSource(
  uri: String,
  crs: CRS,
  resampleMethod: ResampleMethod = NearestNeighbor,
  errorThreshold: Double = 0.125
) extends RasterSource {
  private val baseSpatialReference = {
    val baseDataset: Dataset = GDAL.open(uri)

    val spatialReference = new SpatialReference(baseDataset.GetProjection)

    baseDataset.delete
    spatialReference
  }

  private val targetSpatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(crs.toProj4String)
    spatialReference
  }

  @transient private lazy val vrt: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    val dataset = gdal.AutoCreateWarpedVRT(
      baseDataset,
      baseDataset.GetProjection,
      targetSpatialReference.ExportToWkt,
      GDAL.deriveGDALResampleMethod(resampleMethod),
      errorThreshold
    )

    baseDataset.delete

    dataset
  }

  private lazy val colsLong: Long = vrt.getRasterXSize
  private lazy val rowsLong: Long = vrt.getRasterYSize

  def cols: Int = colsLong.toInt
  def rows: Int = rowsLong.toInt

  private lazy val geoTransform: Array[Double] = vrt.GetGeoTransform

  private lazy val xmin: Double = geoTransform(0)
  private lazy val ymin: Double = geoTransform(3) + geoTransform(5) * rows
  private lazy val xmax: Double = geoTransform(0) + geoTransform(1) * cols
  private lazy val ymax: Double = geoTransform(3)

  lazy val extent = Extent(xmin, ymin, xmax, ymax)
  override lazy val rasterExtent =
    RasterExtent(
      extent,
      geoTransform(1),
      math.abs(geoTransform(5)),
      cols,
      rows
    )

  lazy val bandCount: Int = vrt.getRasterCount

  private lazy val datatype: GDALDataType = {
    val band = vrt.GetRasterBand(1)
    band.getDataType()
  }

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  private lazy val reader = GDALReader(vrt)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] =
    windows.map { case targetRasterExtent =>
      val tile = reader.read(targetRasterExtent.gridBounds)

      Raster(tile, targetRasterExtent.extent)
    }.toIterator

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}
