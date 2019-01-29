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
  strategy: OverviewStrategy = AutoHigherResolution
) extends RasterSource { self =>
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  protected lazy val baseCRS: CRS = tiff.crs
  protected lazy val baseRasterExtent: RasterExtent = tiff.rasterExtent

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)

  override lazy val gridExtent: GridExtent = reprojectOptions.targetRasterExtent match {
      case Some(targetRasterExtent) =>
        targetRasterExtent
      case None =>
        ReprojectRasterExtent(baseRasterExtent, transform, reprojectOptions)
    }

  lazy val resolutions: scala.List[GridExtent] =
      gridExtent :: tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent, transform))

  @transient private[vlm] lazy val closestTiffOverview: GeoTiff[MultibandTile] = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
      tiff.getClosestOverview(estimatedSource.cellSize, strategy)
    } else {
      tiff.getClosestOverview(baseRasterExtent.cellSize, strategy)
    }
  }

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

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

    val windows: Map[GridBounds, (Extent, RasterExtent)] = { for {
      extent <- extents
      targetExtent <- extent.intersection(this.extent)
    } yield {
      val targetRasterExtent = this.gridExtent.createAlignedRasterExtent(targetExtent)
      val sourceFootprint = targetExtent.reprojectAsPolygon(backTransform, this.cellSize.resolution/8)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(sourceFootprint.envelope, clamp = true)
      val sourceExtent = closestTiffOverview.rasterExtent.rasterExtentFor(sourcePixelBounds).extent
      (sourcePixelBounds, (sourceExtent, targetRasterExtent))
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
      val (sourceExtent, targetRasterExtent) = windows(sourcePixelBounds)
      val sourceRaster = Raster(tile, sourceExtent)
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
    }
  }

  def rasterExtentFor(re: RasterExtent, gridBounds: GridBounds): RasterExtent = {
    val (xminCenter, ymaxCenter) = re.gridToMap(gridBounds.colMin, gridBounds.rowMin)
    val (xmaxCenter, yminCenter) = re.gridToMap(gridBounds.colMax, gridBounds.rowMax)
    val (hcw, hch) = (re.cellwidth / 2, re.cellheight / 2)
    val e = Extent(xminCenter - hcw, yminCenter - hch, xmaxCenter + hcw, ymaxCenter + hch)
    RasterExtent(e, re.cellwidth, re.cellheight, gridBounds.width, gridBounds.height)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

    val windows: Map[GridBounds, (Extent, RasterExtent)] = { for {
      targetPixelBounds <- bounds
      targetExtent = this.gridExtent.extentFor(targetPixelBounds, clamp = false)
    } yield {
      val targetRasterExtent = rasterExtentFor(this.gridExtent.toRasterExtent, targetPixelBounds)
      val sourceFootprint = targetExtent.reprojectAsPolygon(backTransform, this.cellSize.resolution/8)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(sourceFootprint.envelope, clamp = true)
      val sourceExtent = closestTiffOverview.rasterExtent.rasterExtentFor(sourcePixelBounds).extent
      (sourcePixelBounds, (sourceExtent, targetRasterExtent))
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
      val (sourceExtent, targetRasterExtent) = windows(sourcePixelBounds)
      val sourceRaster = Raster(tile, sourceExtent)
      val rr = implicitly[RasterRegionReproject[MultibandTile]]
      val raster = rr.regionReproject(
        sourceRaster,
        baseCRS,
        crs,
        targetRasterExtent,
        targetRasterExtent.extent.toPolygon,
        reprojectOptions.method,
        reprojectOptions.errorThreshold
      )
      raster
    }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val resampledReprojectOptions = resampleGrid.reprojectOptions(self.extent, method)
    GeoTiffReprojectRasterSource(uri, crs, resampledReprojectOptions, strategy)
  }
}
