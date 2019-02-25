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

case class GeoTiffReprojectRasterSource(
  uri: String,
  crs: CRS,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource { self =>
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  protected lazy val baseCRS: CRS = tiff.crs
  protected lazy val baseRasterExtent: RasterExtent = tiff.rasterExtent

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)

  override lazy val rasterExtent: RasterExtent = reprojectOptions.targetRasterExtent match {
    case Some(targetRasterExtent) =>
      targetRasterExtent
    case None =>
      ReprojectRasterExtent(baseRasterExtent, transform, reprojectOptions)
  }

  lazy val resolutions: List[RasterExtent] =
    rasterExtent :: tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent, transform))

  @transient private[vlm] lazy val closestTiffOverview: GeoTiff[MultibandTile] = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(rasterExtent, backTransform)
      tiff.getClosestOverview(estimatedSource.cellSize, strategy)
    } else {
      tiff.getClosestOverview(baseRasterExtent.cellSize, strategy)
    }
  }

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val it = readBounds(List(bounds), bands)
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
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]
    val intersectingWindows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this)
    } yield {
      val targetRasterExtent = RasterExtent(rasterExtent.extentFor(targetPixelBounds, clamp = true), targetPixelBounds.width, targetPixelBounds.height)
      val sourceExtent = targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).envelope
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
      (sourcePixelBounds, targetRasterExtent)
    }}.toMap

    geoTiffTile.crop(intersectingWindows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
      val targetRasterExtent = intersectingWindows(sourcePixelBounds)
      val sourceRaster = Raster(tile, closestTiffOverview.rasterExtent.extentFor(sourcePixelBounds, clamp = true))
      val rr = implicitly[RasterRegionReproject[MultibandTile]]
      rr.regionReproject(
        sourceRaster,
        baseCRS,
        crs,
        targetRasterExtent,
        targetRasterExtent.extent.toPolygon,
        reprojectOptions.method,
        reprojectOptions.errorThreshold
      )
    }.map { convertRaster }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, crs, reprojectOptions.copy(method = method, targetRasterExtent = Some(resampleGrid(self.rasterExtent))), strategy, targetCellType)

  def convert(targetCellType: TargetCellType): RasterSource =
    GeoTiffReprojectRasterSource(uri, crs, reprojectOptions, strategy, Some(targetCellType))
}
