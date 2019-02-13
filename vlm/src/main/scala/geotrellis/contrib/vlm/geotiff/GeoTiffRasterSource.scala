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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.ResampleMethod

case class GeoTiffRasterSource(uri: String) extends GeoTiffBaseRasterSource {
  private[geotiff] lazy val parentOptions: RasterViewOptions = RasterViewOptions()
  private[geotiff] lazy val options: RasterViewOptions = RasterViewOptions()

  protected lazy val parentSteps: StepCollection = StepCollection()
  protected lazy val currentStep: Option[Step] = None

  def resampleMethod: Option[ResampleMethod] = None

  val rasterExtent: RasterExtent = baseRasterExtent
  lazy val resolutions: List[RasterExtent] = baseResolutions
  def crs: CRS = baseCRS
  def cellType: CellType = baseCellType
}
