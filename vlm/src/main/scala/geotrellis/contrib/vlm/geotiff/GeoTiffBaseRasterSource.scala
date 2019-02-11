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

trait GeoTiffBaseRasterSource extends RasterSource {
  val uri: String

  def resampleMethod: Option[ResampleMethod]

  private[geotiff] val parentOptions: RasterViewOptions
  private[geotiff] val options: RasterViewOptions

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  lazy val baseRasterExtent: RasterExtent = tiff.rasterExtent
  lazy val baseResolutions: List[RasterExtent] = baseRasterExtent :: tiff.overviews.map(_.rasterExtent)
  def baseCRS: CRS = tiff.crs
  def baseCellType: CellType = tiff.cellType

  def bandCount: Int = tiff.bandCount

  // These are the values of the previous GeoTiffRasterSource
  protected lazy val parentRasterExtent: RasterExtent = parentOptions.rasterExtent.getOrElse(baseRasterExtent)
  protected lazy val parentCRS: CRS = parentOptions.crs.getOrElse(baseCRS)
  protected lazy val parentCellType: CellType = parentOptions.cellType.getOrElse(baseCellType)

  val rasterExtent: RasterExtent
  val resolutions: List[RasterExtent]
  def crs: CRS
  def cellType: CellType

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy)

  def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTiffConvertedRasterSource(uri, cellType, strategy)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
    if (it.hasNext) Some(it.next) else None
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = true))
    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val intersectingBounds = bounds.flatMap(_.intersection(this)).toSeq
    geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = true))
    }
  }
}
