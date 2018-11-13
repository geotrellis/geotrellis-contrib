package geotrellis.contrib.vlm.gdal

import geotrellis.raster.CellSize
import geotrellis.raster.resample.ResampleMethod
import geotrellis.proj4.CRS
import geotrellis.vector.Extent

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
  alignTargetPixels: Boolean = true,
  dimensions: Option[(Int, Int)] = None,
  sourceCRS: Option[SpatialReference] = None,
  targetCRS: Option[SpatialReference] = None,
  te: Option[(Extent, CRS)] = None,
  ovr: Option[String] = Some("AUTO")
) {

  def roundUp(d: Double, digits: Int = 7): Double =
    BigDecimal(d).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toDouble

  def toWarpOptionsList: List[String] = {
    outputFormat.toList.flatMap { of => List("-of", of) } :::
    resampleMethod.toList.flatMap { method => List("-r", s"${GDAL.deriveResampleMethodString(method)}") } :::
    errorThreshold.toList.flatMap { et => List("-et", s"${et}") } :::
    cellSize.toList.flatMap { cz =>
      // the -tap parameter can only be set if -tr is set as well
      val tr = List("-tr", s"${cz.width}", s"${cz.height}")
      if (alignTargetPixels) "-tap" +: tr else tr
    } :::
    dimensions.toList.flatMap { case (c, r) => List("-ts", s"$c", s"$r") } :::
    (sourceCRS, targetCRS).mapN { (source, target) =>
      if(source != target) List("-s_srs", source.ExportToProj4, "-t_srs", target.ExportToProj4)
      else Nil
    }.toList.flatten ::: ovr.toList.flatMap { o => List("-ovr", o) } :::
    te.toList.flatMap { case (ext, crs) =>
      List(
        "-te", s"${ext.xmin}", s"${ext.ymin}", s"${ext.xmax}", s"${ext.ymax}",
        "-te_srs", s"${crs.toProj4String}"
      )
    }
  }

  def toWarpOptions: WarpOptions =
    new WarpOptions(new java.util.Vector(toWarpOptionsList.asJava))
}
