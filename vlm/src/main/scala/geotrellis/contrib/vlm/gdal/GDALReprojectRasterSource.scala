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

import geotrellis.gdal._
import geotrellis.proj4._
import geotrellis.raster.reproject.Reproject
import geotrellis.contrib.vlm.{RasterSource, ResampleGrid}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}

import cats.syntax.option._
import org.gdal.osr.SpatialReference

case class GDALReprojectRasterSource(
  uri: String,
  targetCRS: CRS,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  options: GDALWarpOptions = GDALWarpOptions(),
  baseWarpList: List[GDALWarpOptions] = Nil
) extends GDALBaseRasterSource {
  def resampleMethod: Option[ResampleMethod] = reprojectOptions.method.some

  lazy val warpOptions: GDALWarpOptions = AnyRef.synchronized {
    val baseSpatialReference = {
      val baseDataset = fromBaseWarpList
      val spatialReference = new SpatialReference()
      spatialReference.ImportFromWkt(Option(baseDataset.GetProjection).getOrElse(LatLng.toWKT.get))
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
      srcNoData = noDataValue.toList,
      ovr = strategy.some
    )
  }

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options, warpList).addParentDatasets(getParentDatasets).addParentDataset(dataset)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options, warpList).addParentDatasets(getParentDatasets).addParentDataset(dataset)
}
