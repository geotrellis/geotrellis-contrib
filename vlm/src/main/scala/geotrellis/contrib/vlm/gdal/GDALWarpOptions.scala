/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import cats.implicits._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import org.gdal.gdal.WarpOptions

import scala.collection.JavaConverters._

/**
  * GDALWarpOptions basically should cover https://www.gdal.org/gdalwarp.html
  *
  * This is a basic implementation, we need to cover all the parameters in a future.
  */
case class GDALWarpOptions(
  /** -of, Select the output format. The default is GeoTIFF (GTiff). Use the short format name. */
  outputFormat: Option[String] = Some("VRT"),
  /** -r, Resampling method to use, visit https://www.gdal.org/gdalwarp.html for details. */
  resampleMethod: Option[ResampleMethod] = None,
  /** -et, error threshold for transformation approximation */
  errorThreshold: Option[Double] = None,
  /** -tr, set output file resolution (in target georeferenced units) */
  cellSize: Option[CellSize] = None,
  /** -tap, aligns the coordinates of the extent of the output file to the values of the target resolution,
    * such that the aligned extent includes the minimum extent.
    *
    * In terms of GeoTrellis it's very similar to [[GridExtent]].createAlignedGridExtent:
    *
    * newMinX = floor(envelop.minX / xRes) * xRes
    * newMaxX = ceil(envelop.maxX / xRes) * xRes
    * newMinY = floor(envelop.minY / yRes) * yRes
    * newMaxY = ceil(envelop.maxY / yRes) * yRes
    *
    * if (xRes == 0) || (yRes == 0) than GDAL calculates it using the extent and the cellSize
    * xRes = (maxX - minX) / cellSize.width
    * yRes = (maxY - minY) / cellSize.height
    *
    * If -tap parameter is NOT set, GDAL increases extent by a half of a pixel, to avoid missing points on the border.
    *
    * The actual code reference: https://github.com/OSGeo/gdal/blob/v2.3.2/gdal/apps/gdal_rasterize_lib.cpp#L402-L461
    * The actual part with the -tap logic: https://github.com/OSGeo/gdal/blob/v2.3.2/gdal/apps/gdal_rasterize_lib.cpp#L455-L461
    *
    * The initial PR that introduced that feature in GDAL 1.8.0: https://trac.osgeo.org/gdal/attachment/ticket/3772/gdal_tap.patch
    * A discussion thread related to it: https://lists.osgeo.org/pipermail/gdal-dev/2010-October/thread.html#26209
    *
    */
  alignTargetPixels: Boolean = true,
  dimensions: Option[(Int, Int)] = None, // -ts
  /** -s_srs, source spatial reference set */
  sourceCRS: Option[CRS] = None,
  /** -t_srs, target spatial reference set */
  targetCRS: Option[CRS] = None,
  /** -te, set georeferenced extents of output file to be created (with a CRS specified) */
  te: Option[(Extent, CRS)] = None,
  /** -srcnodata, set nodata masking values for input bands (different values can be supplied for each band) */
  srcNoData: Option[Double] = None,
  /** -ovr,  To specify which overview level of source files must be used.
    *        The default choice, AUTO, will select the overview level whose resolution is the closest to the target resolution.
    *        Specify an integer value (0-based, i.e. 0=1st overview level) to select a particular level.
    *        Specify AUTO-n where n is an integer greater or equal to 1, to select an overview level below the AUTO one.
    *        Or specify NONE to force the base resolution to be used (can be useful if overviews have been generated with a low quality resampling method, and the warping is done using a higher quality resampling method).
    */
  ovr: Option[OverviewStrategy] = Some(AutoHigherResolution)
) {
  lazy val name: String = toWarpOptionsList.map(_.toLowerCase).mkString("_")

  def toWarpOptionsList: List[String] = {
    outputFormat.toList.flatMap { of => List("-of", of) } :::
    resampleMethod.toList.flatMap { method => List("-r", s"${GDALUtils.deriveResampleMethodString(method)}") } :::
    errorThreshold.toList.flatMap { et => List("-et", s"${et}") } :::
    cellSize.toList.flatMap { cz =>
      // the -tap parameter can only be set if -tr is set as well
      val tr = List("-tr", s"${cz.width}", s"${cz.height}")
      if (alignTargetPixels) "-tap" +: tr else tr
    } ::: dimensions.toList.flatMap { case (c, r) => List("-ts", s"$c", s"$r") } :::
    (sourceCRS, targetCRS).mapN { (source, target) =>
      if(source != target) List("-s_srs", source.toProj4String, "-t_srs", target.toProj4String)
      else Nil
    }.toList.flatten ::: ovr.toList.flatMap { o => List("-ovr", GDALUtils.deriveOverviewStrategyString(o)) } :::
    te.toList.flatMap { case (ext, crs) =>
      List(
        "-te", s"${ext.xmin}", s"${ext.ymin}", s"${ext.xmax}", s"${ext.ymax}",
        "-te_srs", s"${crs.toProj4String}"
      )
    } ::: srcNoData.toList.flatMap { nd => List("-srcnodata", s"${nd}") }
  }

  def toWarpOptions: WarpOptions =
    new WarpOptions(new java.util.Vector(toWarpOptionsList.asJava))

  def combine(that: GDALWarpOptions): GDALWarpOptions = {
    if (that == this) this
    else this.copy(
      outputFormat orElse that.outputFormat,
      resampleMethod orElse that.resampleMethod,
      errorThreshold orElse that.errorThreshold,
      cellSize orElse that.cellSize,
      alignTargetPixels,
      dimensions orElse that.dimensions,
      sourceCRS orElse that.sourceCRS,
      targetCRS orElse that.targetCRS,
      te orElse that.te,
      srcNoData orElse that.srcNoData,
      ovr orElse that.ovr
    )
  }
}
