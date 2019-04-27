/*
 * Copyright 2019 Azavea
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

import com.azavea.gdal.GDALWarp
import geotrellis.contrib.vlm.TargetCellType
import geotrellis.raster.GridExtent
import geotrellis.raster.resample.ResampleMethod

case class GDALRasterSource(
  uri: String,
  options: GDALWarpOptions = GDALWarpOptions(),
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends GDALBaseRasterSource {
  val baseWarpList: List[GDALWarpOptions] = Nil
  def resampleMethod: Option[ResampleMethod] = None
  lazy val warpOptions: GDALWarpOptions = options

  override lazy val gridExtent: GridExtent[Long] =
    if(options.isDefault) dataset.rasterExtent(GDALWarp.SOURCE).toGridType[Long]
    else dataset.rasterExtent(GDALWarp.WARPED).toGridType[Long]

  override lazy val resolutions: List[GridExtent[Long]] =
    if(options.isDefault) dataset.resolutions(GDALWarp.SOURCE).map(_.toGridType[Long])
    else dataset.resolutions(GDALWarp.WARPED).map(_.toGridType[Long])
}
