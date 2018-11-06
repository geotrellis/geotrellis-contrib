package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.raster._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject

import cats.implicits._

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  baseWarpList: List[GDALWarpOptions] = Nil
) extends GDALBaseRasterSource {
  lazy val warpOptions: GDALWarpOptions = {
    val res = resampleGrid match {
      case Dimensions(cols, rows) =>
        GDALWarpOptions(
          dimensions = (cols, rows).some,
          resampleMethod = method.some
        )
      case _ =>
        lazy val rasterExtent: RasterExtent = {
          val baseDataset = fromBaseWarpList
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
        // raster extent won't be calculated if it's not called in the apply function body explicitly
        val targetRasterExtent = resampleGrid(rasterExtent)
        GDALWarpOptions(
          cellSize = targetRasterExtent.cellSize.some,
          alignTargetPixels = false,
          resampleMethod = method.some
        )
    }

    res
  }

  override def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, options, warpList)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, warpList)
}
