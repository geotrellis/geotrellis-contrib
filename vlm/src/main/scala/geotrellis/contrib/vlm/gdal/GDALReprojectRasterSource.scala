package geotrellis.contrib.vlm.gdal

import geotrellis.proj4._
import geotrellis.raster.reproject.Reproject
import geotrellis.contrib.vlm.{RasterSource, ResampleGrid}
import geotrellis.raster.resample.ResampleMethod

import cats.implicits._
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference

case class GDALReprojectRasterSource(
  uri: String,
  targetCRS: CRS,
  options: Reproject.Options = Reproject.Options.DEFAULT
) extends GDALBaseRasterSource { self =>
  @transient lazy val dataset: Dataset = {
    val baseDataset = self.baseDataset
    val baseSpatialReference = new SpatialReference(baseDataset.GetProjection)
    val targetSpatialReference: SpatialReference = {
      val spatialReference = new SpatialReference()
      spatialReference.ImportFromProj4(targetCRS.toProj4String)
      spatialReference
    }

    val cellSize = options.targetRasterExtent.map(_.cellSize) match {
      case sz if sz.nonEmpty => sz
      case _ => options.targetCellSize match {
        case sz if sz.nonEmpty => sz
        case _ => options.parentGridExtent.map(_.toRasterExtent().cellSize)
      }
    }

    val warpOptions =
      GDALWarpOptions(
        resampleMethod = options.method.some,
        errorThreshold = options.errorThreshold.some,
        cellSize       = cellSize,
        alignTargetPixels = true,
        sourceCRS      = baseSpatialReference.some,
        targetCRS      = targetSpatialReference.some
      )

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
