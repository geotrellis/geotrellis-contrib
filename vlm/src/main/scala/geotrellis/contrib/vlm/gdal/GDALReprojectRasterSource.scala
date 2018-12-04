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
  def resampleMethod: Option[ResampleMethod] = options.method.some

  lazy val warpOptions: GDALWarpOptions = {
    val baseSpatialReference = {
      val baseDataset = fromBaseWarpList
      val result = new SpatialReference(baseDataset.GetProjection)
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
      sourceCRS = baseSpatialReference.toCRS.some,
      targetCRS = targetSpatialReference.toCRS.some,
      srcNoData = noDataValue
    )

    res
  }

  override def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, options, warpList)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, warpList)
}
