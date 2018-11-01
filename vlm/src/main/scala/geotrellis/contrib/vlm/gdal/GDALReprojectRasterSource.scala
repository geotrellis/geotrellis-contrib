package geotrellis.contrib.vlm.gdal

import geotrellis.proj4._
import geotrellis.raster.reproject.Reproject
import geotrellis.contrib.vlm.{RasterSource, ResampleGrid}
import geotrellis.raster.resample.ResampleMethod

import cats.implicits._
import org.gdal.osr.SpatialReference

case class GDALReprojectRasterSource(
  uri: String,
  targetCRS: CRS,
  options: Reproject.Options = Reproject.Options.DEFAULT,
  baseWarpList: List[GDALWarpOptions] = Nil
) extends GDALBaseRasterSource {
  lazy val warpOptions: GDALWarpOptions = {
    val baseSpatialReference = {
      val baseDataset = fromBaseWarpList
      val result = new SpatialReference(baseDataset.GetProjection)
      baseDataset.delete()
      result
    }
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

    val res = GDALWarpOptions(
      resampleMethod = options.method.some,
      errorThreshold = options.errorThreshold.some,
      cellSize = cellSize,
      alignTargetPixels = true,
      sourceCRS = baseSpatialReference.some,
      targetCRS = targetSpatialReference.some
    )

    res
  }

  override lazy val warpList: List[GDALWarpOptions] = baseWarpList :+ warpOptions

  override def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    try GDALReprojectRasterSource(uri, targetCRS, options, warpList) finally this.close

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    try GDALResampleRasterSource(uri, resampleGrid, method, warpList) finally this.close
}
