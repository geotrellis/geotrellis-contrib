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
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffRasterSource(uri: String) extends GeoTiffBaseRasterSource {
  private[geotiff] lazy val parentOptions: RasterViewOptions = RasterViewOptions()
  private[geotiff] lazy val options: RasterViewOptions = RasterViewOptions()

  def resampleMethod: Option[ResampleMethod] = None

  val rasterExtent: RasterExtent = baseRasterExtent
  val resolutions: List[RasterExtent] = baseResolutions
  def crs: CRS = baseCRS
  def cellType: CellType = baseCellType
}
