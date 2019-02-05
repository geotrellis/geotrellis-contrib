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
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffConvertedRasterSource(
  uri: String,
  cellType: CellType,
  strategy: OverviewStrategy = AutoHigherResolution
) extends RasterSource { self =>
  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def bandCount: Int = tiff.bandCount
  def crs: CRS = tiff.crs
  def rasterExtent: RasterExtent = tiff.rasterExtent

  lazy val resolutions: List[RasterExtent] = rasterExtent :: tiff.overviews.map(_.rasterExtent)

  def resampleMethod: Option[ResampleMethod] = None

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) {
      val raster = it.next

      Some(raster.copy(tile = raster.tile.convert(cellType)))
    } else
      None
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) {
      val raster = it.next

      Some(raster.copy(tile = raster.tile.convert(cellType)))
    } else
      None
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, crs, Reproject.Options(method = method, targetRasterExtent = Some(resampleGrid(self.rasterExtent))), strategy)

  def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTiffConvertedRasterSource(uri, cellType, strategy)
}
