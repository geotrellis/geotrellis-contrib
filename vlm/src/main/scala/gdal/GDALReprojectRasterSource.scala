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
  private lazy val baseSpatialReference = {
    val baseDataset: Dataset = GDAL.open(uri)
    val ref = new SpatialReference(baseDataset.GetProjection)
    baseDataset.delete()
    ref
  }

  private lazy val targetSpatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(targetCRS.toProj4String)
    spatialReference
  }

  @transient lazy val dataset: Dataset = {
    val warpOptions =
      GDALWarpOptions(
        resampleMethod = options.method.some,
        errorThreshold = options.errorThreshold.some,
        cellSize       = options.targetRasterExtent.map(_.cellSize).fold(options.targetCellSize)(Some(_)),
        sourceCRS      = baseSpatialReference.some,
        targetCRS      = targetSpatialReference.some
      )

    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)
    val dataset = gdal.Warp("", Array(baseDataset), warpOptions.toWarpOptions)
    baseDataset.delete
    dataset
  }
}
