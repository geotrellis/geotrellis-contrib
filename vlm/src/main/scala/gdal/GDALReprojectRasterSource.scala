package geotrellis.contrib.vlm.gdal

import geotrellis.proj4._
import geotrellis.raster.reproject.Reproject

import cats.implicits._
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference

case class GDALReprojectRasterSource(
  uri: String,
  targetCRS: CRS,
  options: Reproject.Options = Reproject.Options.DEFAULT
) extends GDALBaseRasterSource {
  @transient lazy val dataset: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

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
        case _ => options.parentGridExtent.map(_.cellSize)
      }
    }

    val warpOptions =
      GDALWarpOptions(
        resampleMethod = options.method.some,
        errorThreshold = options.errorThreshold.some,
        cellSize       = cellSize,
        sourceCRS      = baseSpatialReference.some,
        targetCRS      = targetSpatialReference.some
      )

    val dataset = gdal.Warp("", Array(baseDataset), warpOptions.toWarpOptions)
    baseDataset.delete
    dataset
  }
}
