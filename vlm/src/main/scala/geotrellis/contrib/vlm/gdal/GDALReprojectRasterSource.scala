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

import geotrellis.contrib.vlm.{Step, ReprojectStep, StepCollection}

import geotrellis.gdal._
import geotrellis.proj4._
import geotrellis.raster.CellType
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}

import cats.syntax.option._
import org.gdal.osr.SpatialReference

case class GDALReprojectRasterSource(
  uri: String,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[gdal] val options: GDALWarpOptions = GDALWarpOptions(),
  private[gdal] val baseWarpList: List[GDALWarpOptions] = Nil,
  protected val parentSteps: StepCollection = StepCollection()
) extends GDALBaseRasterSource {
  val targetCRS: CRS = options.targetCRS.get

  def resampleMethod: Option[ResampleMethod] = reprojectOptions.method.some

  protected lazy val currentStep: Option[Step] = Some(ReprojectStep(crs, targetCRS, reprojectOptions))

  lazy private[gdal] val warpOptions: GDALWarpOptions = {
    val baseSpatialReference = {
      val spatialReference = new SpatialReference()
      spatialReference.ImportFromWkt(baseDataset.getProjection.getOrElse(LatLng.toWKT.get))
      spatialReference
    }
    val targetSpatialReference = {
      val spatialReference = new SpatialReference()
      spatialReference.ImportFromProj4(targetCRS.toProj4String)
      spatialReference
    }

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
      cellSize = cellSize,
      sourceCRS = baseSpatialReference.toCRS.some,
      targetCRS = targetSpatialReference.toCRS.some,
      srcNoData = noDataValue.map { _.toString }.toList,
      ovr = strategy.some
    )
  }
}

object GDALReprojectRasterSource {
  def apply(uri: String, targetCRS: CRS): GDALReprojectRasterSource =
    GDALRasterSource(uri).reproject(targetCRS).asInstanceOf[GDALReprojectRasterSource]
}
