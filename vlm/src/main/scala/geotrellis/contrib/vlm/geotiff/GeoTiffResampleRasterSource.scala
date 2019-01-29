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
  strategy: OverviewStrategy = AutoHigherResolution
) extends RasterSource { self =>
  def resampleMethod: Option[ResampleMethod] = Some(method)

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def crs: CRS = tiff.crs
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  override lazy val gridExtent: GridExtent = resampleGrid(tiff.rasterExtent)
  lazy val resolutions: List[GridExtent] = {
      val ratio = gridExtent.cellSize.resolution / tiff.rasterExtent.cellSize.resolution
      gridExtent :: tiff.overviews.map { ovr =>
        val re = ovr.rasterExtent
        val CellSize(cw, ch) = re.cellSize
        RasterExtent(re.extent, CellSize(cw * ratio, ch * ratio))
      }
    }

  @transient protected lazy val closestTiffOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(gridExtent.cellSize, strategy)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    new GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy) {
      override lazy val gridExtent: GridExtent = reprojectOptions.targetRasterExtent match {
              case Some(targetRasterExtent) => targetRasterExtent
              case None => ReprojectRasterExtent(self.gridExtent, this.transform, this.reprojectOptions)
            }
    }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readExtents(List(extent), bands)
    if (it.hasNext) Some(it.next) else None
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

    val windows: Map[GridBounds, (RasterExtent, RasterExtent)] = { for {
      extent <- extents
      intersectingExtent <- extent.intersection(this.extent)
    } yield {
      val targetRasterExtent = this.gridExtent.createAlignedRasterExtent(intersectingExtent)
      val sourceRaterExtent = closestTiffOverview.rasterExtent.createAlignedRasterExtent(intersectingExtent)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
      (sourcePixelBounds, (sourceRaterExtent, targetRasterExtent))
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
      val (sourceRasterExtent, targetRasterExtent) = windows(gb)
      Raster(
        tile = tile,
        extent = sourceRasterExtent.extent
      ).resample(targetRasterExtent, method)
    }
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

    val windows: Map[GridBounds, (RasterExtent, RasterExtent)] = { for {
      targetPixelBounds: GridBounds <- bounds
      extent = this.gridExtent.extentFor(targetPixelBounds, clamp = false)
      intersectingExtent <- extent.intersection(this.extent)
    } yield {
      val targetRasterExtent = this.gridExtent.createAlignedRasterExtent(intersectingExtent)
      val sourceRaterExtent = closestTiffOverview.rasterExtent.createAlignedRasterExtent(intersectingExtent)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
      (sourcePixelBounds, (sourceRaterExtent, targetRasterExtent))
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
      val (sourceRasterExtent, targetRasterExtent) = windows(gb)
      Raster(
        tile = tile,
        extent = sourceRasterExtent.extent
      ).resample(targetRasterExtent, method)
    }
  }
}
