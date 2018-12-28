package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.{RasterSource, ResampleGrid}
import geotrellis.gdal.{GDAL, GDALDataset, GDALWarpOptions}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod

trait GDALWarping { self: GDALBaseRasterSource =>
  /** options to override some values on transformation steps, should be used carefully as these params can change the behaviour significantly */
  val options: GDALWarpOptions
  /** options from previous transformation steps */
  val baseWarpList: List[GDALWarpOptions]
  /** current transformation options */
  val warpOptions: GDALWarpOptions
  /** the list of transformation options including the current one */
  lazy val warpList: List[GDALWarpOptions] = baseWarpList :+ warpOptions

  // generate a vrt before the current options application
  @transient lazy val fromBaseWarpList: GDALDataset = GDAL.fromGDALWarpOptions(uri, baseWarpList)
  // generate a vrt with the current options application
  @transient lazy val fromWarpList: GDALDataset = GDAL.fromGDALWarpOptions(uri, warpList)

  // current dataset
  @transient lazy val dataset: GDALDataset = fromWarpList

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = fromBaseWarpList.getRasterBand(1).getNoDataValue

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options)
}
