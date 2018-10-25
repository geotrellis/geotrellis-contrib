package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.raster._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject

import cats.implicits._
import org.gdal.gdal.{Dataset, gdal}

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor
) extends GDALBaseRasterSource { self =>
  @transient lazy val dataset: Dataset = {
    val baseDataset = self.baseDataset
    val warpOptions =
      resampleGrid match {
        case Dimensions(cols, rows) =>
          GDALWarpOptions(
            dimensions = (cols, rows).some,
            resampleMethod = method.some
          )
        case _ =>
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
          GDALWarpOptions(
            cellSize = targetRasterExtent.cellSize.some,
            alignTargetPixels = false,
            resampleMethod = method.some
          )
      }

    val dataset = gdal.Warp("", Array(baseDataset), warpOptions.toWarpOptions)
    baseDataset.delete
    dataset
  }

  override def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    new GDALReprojectRasterSource(uri, targetCRS, options) {
      override def baseDataset: Dataset = self.dataset
    }

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    new GDALResampleRasterSource(uri, resampleGrid, method) {
      override def baseDataset: Dataset = self.dataset
    }
}
