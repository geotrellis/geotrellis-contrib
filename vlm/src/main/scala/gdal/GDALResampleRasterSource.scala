package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.raster._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._

import org.gdal.gdal.{Dataset, gdal}
import cats.implicits._

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor
) extends GDALBaseRasterSource {
  @transient lazy val dataset: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    val rasterExtent: RasterExtent = {
      val colsLong: Long = baseDataset.getRasterXSize
      val rowsLong: Long = baseDataset.getRasterYSize

      val cols: Int = colsLong.toInt
      val rows: Int = rowsLong.toInt

      val geoTransform: Array[Double] = baseDataset.GetGeoTransform

      val xmin: Double = geoTransform(0)
      val ymin: Double = geoTransform(3) + geoTransform(5) * rows
      val xmax: Double = geoTransform(0) + geoTransform(1) * cols
      val ymax: Double = geoTransform(3)

      RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
    }

    val targetRasterExtent = resampleGrid(rasterExtent)

    val warpOptions =
      GDALWarpOptions(
        cellSize       = targetRasterExtent.cellSize.some,
        resampleMethod = method.some
      )

    val dataset = gdal.Warp("", Array(baseDataset), warpOptions.toWarpOptions)
    baseDataset.delete
    dataset
  }
}
