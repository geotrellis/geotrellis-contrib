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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._
import geotrellis.vector.{Extent}
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffReprojectRasterSource(
  dataPath: GeoTiffDataPath,
  crs: CRS,
  targetResampleGrid: Option[ResampleGrid[Long]] = None,
  resampleMethod: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  errorThreshold: Double = 0.125,
  private[vlm] val targetCellType: Option[TargetCellType] = None,
  private val baseTiff: Option[MultibandGeoTiff] = None
) extends RasterSource { self =>
  @transient lazy val tiff: MultibandGeoTiff =
    baseTiff.getOrElse(GeoTiffReader.readMultiband(getByteReader(dataPath.path), streaming = true))

  protected lazy val baseCRS: CRS = tiff.crs
  protected lazy val baseGridExtent: GridExtent[Long] = tiff.rasterExtent.toGridType[Long]

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)
  
  override lazy val gridExtent: GridExtent[Long] = {
    lazy val reprojectedRasterExtent =
      ReprojectRasterExtent(
        baseGridExtent,
        transform,
        Reproject.Options.DEFAULT.copy(method = resampleMethod, errorThreshold = errorThreshold)
      )

    targetResampleGrid match {
      case Some(targetRegion: TargetRegion[Long]) => targetRegion.region
      case Some(targetGrid: TargetGrid[Long]) => targetGrid(reprojectedRasterExtent)
      case Some(dimensions: Dimensions[Long]) => dimensions(reprojectedRasterExtent)
      case Some(targetCellSize: TargetCellSize[Long]) => targetCellSize(reprojectedRasterExtent)
      case _ => reprojectedRasterExtent
    }
  }

  lazy val resolutions: List[GridExtent[Long]] =
      gridExtent :: tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent.toGridType[Long], transform))

  @transient private[vlm] lazy val closestTiffOverview: GeoTiff[MultibandTile] = {
    if(targetResampleGrid.isDefined) {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
      tiff.getClosestOverview(estimatedSource.cellSize, strategy)
    } else {
      tiff.getClosestOverview(baseGridExtent.cellSize, strategy)
    }
  }

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)

    read(bounds, bands)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)

    closestTiffOverview.synchronized { if (it.hasNext) Some(it.next) else None }
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = true))

    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]
    val intersectingWindows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this.gridBounds)
    } yield {
      val targetRasterExtent = RasterExtent(
        extent = gridExtent.extentFor(targetPixelBounds, clamp = true),
        cols = targetPixelBounds.width.toInt,
        rows = targetPixelBounds.height.toInt
      )

      // A tmp workaround for https://github.com/locationtech/proj4j/pull/29
      // Stacktrace details: https://github.com/geotrellis/geotrellis-contrib/pull/206#pullrequestreview-260115791
      val sourceExtent = Proj4Transform.synchronized(targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).envelope)
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
        resampleMethod,
        errorThreshold
      )
    }.map { convertRaster }
  }

  def reprojection(targetCRS: CRS, resampleGrid: Option[ResampleGrid[Long]] = None, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, resampleGrid, method, strategy, targetCellType = targetCellType, baseTiff = Some(tiff))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffReprojectRasterSource(
      uri,
      crs,
      Some(resampleGrid),
      method,
      strategy,
      targetCellType = targetCellType,
      baseTiff = Some(tiff)
    )

  def convert(targetCellType: TargetCellType): RasterSource =
    GeoTiffReprojectRasterSource(uri, crs, targetResampleGrid, resampleMethod, strategy, targetCellType = Some(targetCellType))
}
