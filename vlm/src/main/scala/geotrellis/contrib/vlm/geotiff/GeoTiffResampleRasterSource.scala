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
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[geotiff] val parentOptions: RasterViewOptions = RasterViewOptions()
) extends GeoTiffBaseRasterSource {
  def resampleMethod: Option[ResampleMethod] = Some(method)

  def crs: CRS = parentCRS
  def cellType: CellType = parentCellType

  override lazy val rasterExtent: RasterExtent = resampleGrid(parentRasterExtent)
  lazy val resolutions: List[RasterExtent] = {
    val ratio = rasterExtent.cellSize.resolution / tiff.rasterExtent.cellSize.resolution
    rasterExtent :: tiff.overviews.map { ovr =>
      val re = ovr.rasterExtent
      val CellSize(cw, ch) = re.cellSize
      RasterExtent(re.extent, CellSize(cw * ratio, ch * ratio))
    }
  }

  private[geotiff] val options = parentOptions.copy(rasterExtent = Some(rasterExtent))

  @transient protected lazy val closestTiffOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(rasterExtent.cellSize, strategy)

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    new GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options) {
      override lazy val rasterExtent: RasterExtent = reprojectOptions.targetRasterExtent match {
        case Some(targetRasterExtent) => targetRasterExtent
        case None => ReprojectRasterExtent(rasterExtent, this.transform, this.reprojectOptions)
      }
    }

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, options)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next.mapTile { _.convert(cellType) }) else None
  }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next.mapTile { _.convert(cellType) }) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val targetPixelBounds = extents.map(rasterExtent.gridBoundsFor(_))
    // result extents may actually expand to cover pixels at our resolution
    // TODO: verify the logic here, should the sourcePixelBounds be calculated from input or expanded extent?
    readBounds(targetPixelBounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

    val windows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this)
    } yield {
      val targetExtent = rasterExtent.extentFor(targetPixelBounds)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetExtent, clamp = true)
      val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width, targetPixelBounds.height)
      (sourcePixelBounds, targetRasterExtent)
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
      val targetRasterExtent = windows(gb)
      Raster(
        tile = tile,
        extent = targetRasterExtent.extent
      ).resample(targetRasterExtent.cols, targetRasterExtent.rows, method)
    }
  }

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTiffConvertedRasterSource(uri, cellType, strategy, options)
}
