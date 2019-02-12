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

import cats.syntax.option._
import org.gdal.gdal.Dataset

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  options: GDALWarpOptions = GDALWarpOptions(),
  baseWarpList: List[GDALWarpOptions] = Nil,
  @transient parentDatasets: Array[Dataset] = Array()
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
        lazy val rasterExtent: RasterExtent = baseDataset.rasterExtent
        // raster extent won't be calculated if it's not called in the apply function body explicitly
        val targetRasterExtent = {
          val re = resampleGrid(rasterExtent).toRasterExtent
          if(options.alignTargetPixels) re.alignTargetPixels else re
        }
        GDALWarpOptions(
          te             = targetRasterExtent.extent.some,
          cellSize       = targetRasterExtent.cellSize.some,
          resampleMethod = resampleMethod,
          srcNoData      = noDataValue.map(_.toString).toList,
          ovr            = strategy.some
        )
    }

    options combine res
  }
}
