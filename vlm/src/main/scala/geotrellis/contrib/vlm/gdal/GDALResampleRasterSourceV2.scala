package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.gdal._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._

case class GDALResampleRasterSourceV2(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution
) extends GDALBaseRasterSource {
  @transient lazy val dataset: GDALDataset = GDAL.fromGDALWarpOptions(uri, Nil)

  def resampleMethod: Option[ResampleMethod] = Some(method)

  /** RasterExtent of the source dataset at base resolution */
  private lazy val datasetRasterExtent: RasterExtent = {
    val colsLong: Long = dataset.getRasterXSize
    val rowsLong: Long = dataset.getRasterYSize

    val cols: Int = colsLong.toInt
    val rows: Int = rowsLong.toInt

    val xmin: Double = geoTransform(0)
    val ymin: Double = geoTransform(3) + geoTransform(5) * rows
    val xmax: Double = geoTransform(0) + geoTransform(1) * cols
    val ymax: Double = geoTransform(3)

    RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
  }

  override lazy val rasterExtent: RasterExtent = resampleGrid(datasetRasterExtent)

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds
      .toIterator
      .flatMap { gb => gridBounds.intersection(gb) }
      .map { targetGridBounds =>
        val targetExtent: Extent = rasterExtent.extentFor(targetGridBounds)
        // bounds are relative to my raster extent, which means I need to translate them to base bounds
        val sourceGridBounds = datasetRasterExtent.gridBoundsFor(targetExtent, clamp = false)
        val raster = reader.readRaster(sourceGridBounds,
          bufXSize = Some(targetGridBounds.width),
          bufYSize = Some(targetGridBounds.height),
          bands)

        // NOTE: I believe its more correct to return extent of pixels at advertised resolution rather than
        //       extent of the pixels at base resolutions that have been sampled.
        Raster(raster.tile, targetExtent)
      }
  }

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, GDALWarpOptions(), Nil)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSourceV2(uri, resampleGrid, method, strategy)
}
