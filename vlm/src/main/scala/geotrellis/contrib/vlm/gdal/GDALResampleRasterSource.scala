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

import geotrellis.contrib.vlm._
import geotrellis.gdal._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject

import cats.syntax.option._

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  options: GDALWarpOptions = GDALWarpOptions(),
  baseWarpList: List[GDALWarpOptions] = Nil
) extends GDALBaseRasterSource {
  def resampleMethod: Option[ResampleMethod] = method.some

  lazy val warpOptions: GDALWarpOptions = {
    val res = resampleGrid match {
      case Dimensions(cols, rows) =>
        GDALWarpOptions(
          dimensions = (cols, rows).some,
          resampleMethod = resampleMethod
        )
      case _ =>
        lazy val rasterExtent: RasterExtent = fromBaseWarpList.rasterExtent
        // raster extent won't be calculated if it's not called in the apply function body explicitly
        val targetRasterExtent = resampleGrid(rasterExtent)
        GDALWarpOptions(
          cellSize = targetRasterExtent.cellSize.some,
          resampleMethod = resampleMethod,
          srcNoData = noDataValue,
          ovr = strategy.some
        )
    }

    options combine res
  }

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options, warpList)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options, warpList)
}
