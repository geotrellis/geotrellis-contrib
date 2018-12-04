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
import geotrellis.raster._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject

import cats.implicits._

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  baseWarpList: List[GDALWarpOptions] = Nil,
  alignTargetPixels: Boolean = true
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
        lazy val rasterExtent: RasterExtent = {
          val baseDataset = fromBaseWarpList
          val colsLong: Long = baseDataset.getRasterXSize
          val rowsLong: Long = baseDataset.getRasterYSize

          val cols: Int = colsLong.toInt
          val rows: Int = rowsLong.toInt

          val geoTransform: Array[Double] = baseDataset.GetGeoTransform

          val xmin: Double = geoTransform(0)
          val ymin: Double = geoTransform(3) + geoTransform(5) * rows
          val xmax: Double = geoTransform(0) + geoTransform(1) * cols
          val ymax: Double = geoTransform(3)

          RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
        }
        // raster extent won't be calculated if it's not called in the apply function body explicitly
        val targetRasterExtent = resampleGrid(rasterExtent)
        GDALWarpOptions(
          cellSize = targetRasterExtent.cellSize.some,
          alignTargetPixels = alignTargetPixels,
          resampleMethod = resampleMethod,
          srcNoData = noDataValue
        )
    }

    res
  }

  override def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, options, warpList, alignTargetPixels)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, warpList, alignTargetPixels)
}
