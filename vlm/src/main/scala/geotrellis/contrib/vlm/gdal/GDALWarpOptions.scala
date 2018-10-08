package geotrellis.contrib.vlm.gdal

import geotrellis.raster.CellSize
import geotrellis.raster.resample.ResampleMethod
import cats.implicits._

import org.gdal.gdal.WarpOptions
import org.gdal.osr.SpatialReference

import scala.collection.JavaConverters._

// TODO: to implement the full coverage of the GDAL API here
case class GDALWarpOptions(
  outputFormat: Option[String] = Some("VRT"),
  resampleMethod: Option[ResampleMethod] = None,
  errorThreshold: Option[Double] = None,
  cellSize: Option[CellSize] = None,
  dimensions: Option[(Int, Int)] = None,
  sourceCRS: Option[SpatialReference] = None,
  targetCRS: Option[SpatialReference] = None
) {

  def roundUp(d: Double, digits: Int = 7): Double =
    BigDecimal(d).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toDouble

  def toWarpOptionsList: List[String] = {
    outputFormat.toList.flatMap { of => List("-of", of) } :::
    resampleMethod.toList.flatMap { method => List("-r", s"${GDAL.deriveResampleMethodString(method)}") } :::
    errorThreshold.toList.flatMap { et => List("-et", s"${et})") } :::
    cellSize.toList.flatMap { cz => List("-tap", "-tr", /*"-crop_to_cutline",*/ s"${cz.width}", s"${cz.height}") } :::
    dimensions.toList.flatMap { case (c, r) => List("-ts", s"$c", s"$r") } :::
    ((sourceCRS, targetCRS).mapN { (source, target) =>
      if(source != target) List("-s_srs", source.ExportToProj4, "-t_srs", target.ExportToProj4)
      else Nil
    }).toList.flatten
  }

  def toWarpOptions: WarpOptions =
    new WarpOptions(new java.util.Vector(toWarpOptionsList.asJava))
}
