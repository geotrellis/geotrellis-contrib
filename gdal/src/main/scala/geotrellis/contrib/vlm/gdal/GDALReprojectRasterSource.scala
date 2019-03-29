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

import geotrellis.proj4._
import geotrellis.contrib.vlm._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}

import com.azavea.gdal.GDALWarp

import cats.syntax.option._

case class GDALReprojectRasterSource(
  uri: String,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  options: GDALWarpOptions = GDALWarpOptions(),
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends GDALBaseRasterSource {
  val targetCRS: CRS = options.targetCRS.get

  def resampleMethod: Option[ResampleMethod] = reprojectOptions.method.some

  lazy val warpOptions: GDALWarpOptions = {
    val baseSpatialReference = dataset.crs(GDALWarp.SOURCE)
    val targetSpatialReference = targetCRS

    val cellSize = reprojectOptions.targetRasterExtent.map(_.cellSize) match {
      case sz if sz.nonEmpty => sz
      case _ => reprojectOptions.targetCellSize match {
        case sz if sz.nonEmpty => sz
        case _ => reprojectOptions.parentGridExtent.map(_.toRasterExtent().cellSize)
      }
    }

    options combine GDALWarpOptions(
      resampleMethod = reprojectOptions.method.some,
      errorThreshold = reprojectOptions.errorThreshold.some,
      cellSize       = cellSize,
      sourceCRS      = baseSpatialReference.some,
      targetCRS      = targetSpatialReference.some,
      srcNoData      = noDataValue.map(_.toString).toList,
      ovr            = strategy.some
    )
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    val convertOptions = GDALBaseRasterSource.createConvertOptions(targetCellType, noDataValue)
    val targetOptions = (convertOptions :+ warpOptions).reduce { _ combine _ }
    GDALReprojectRasterSource(uri, reprojectOptions, strategy, targetOptions, Some(targetCellType))
  }
}

object GDALReprojectRasterSource {
  def apply(uri: String, targetCRS: CRS): GDALReprojectRasterSource =
    GDALRasterSource(uri).reproject(targetCRS).asInstanceOf[GDALReprojectRasterSource]
}
