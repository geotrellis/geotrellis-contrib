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
import geotrellis.vector.Extent
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy, Tags}
import geotrellis.util.RangeReader

case class GeoTiffResampleRasterSource(
  dataPath: GeoTiffPath,
  resampleGrid: ResampleGrid[Long],
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None,
  @transient private[vlm] val baseTiff: Option[MultibandGeoTiff] = None
) extends RasterSource {
  def resampleMethod: Option[ResampleMethod] = Some(method)
  def name: GeoTiffPath = dataPath

  @transient lazy val tiff: MultibandGeoTiff =
    Option(baseTiff).flatten.getOrElse(GeoTiffReader.readMultiband(RangeReader(dataPath.value), streaming = true))

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)
  def tags: Tags = tiff.tags
  def metadata: GeoTiffMetadata = GeoTiffMetadata(name, crs, bandCount, cellType, gridExtent, resolutions, tags)

  /** Returns the GeoTiff head tags. */
  def attributes: Map[String, String] = tags.headTags
  /** Returns the GeoTiff per band tags. */
  def attributesForBand(band: Int): Map[String, String] = tags.bandTags.lift(band).getOrElse(Map.empty)

  def crs: CRS = tiff.crs

  override lazy val gridExtent: GridExtent[Long] = resampleGrid(tiff.rasterExtent.toGridType[Long])
  lazy val resolutions: List[GridExtent[Long]] = {
    val ratio = gridExtent.cellSize.resolution / tiff.rasterExtent.cellSize.resolution
    gridExtent :: tiff.overviews.map { ovr =>
      val re = ovr.rasterExtent
      val CellSize(cw, ch) = re.cellSize
      new GridExtent[Long](re.extent, CellSize(cw * ratio, ch * ratio))
    }
  }

  @transient protected lazy val closestTiffOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(gridExtent.cellSize, strategy)

  def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): GeoTiffReprojectRasterSource =
    new GeoTiffReprojectRasterSource(dataPath, targetCRS, resampleGrid, method, strategy, targetCellType = targetCellType) {
      override lazy val gridExtent: GridExtent[Long] = {
        val reprojectedRasterExtent =
          ReprojectRasterExtent(
            baseGridExtent,
            transform,
            Reproject.Options.DEFAULT.copy(method = resampleMethod, errorThreshold = errorThreshold)
          )

        targetResampleGrid match {
          case targetRegion: TargetRegion[Long] => targetRegion.region
          case targetGrid: TargetGrid[Long] => targetGrid(reprojectedRasterExtent)
          case dimensions: Dimensions[Long] => dimensions(reprojectedRasterExtent)
          case targetCellSize: TargetCellSize[Long] => targetCellSize(reprojectedRasterExtent)
          case _ => reprojectedRasterExtent
        }
      }
    }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeoTiffResampleRasterSource(dataPath, resampleGrid, method, strategy, targetCellType, Some(tiff))

  def convert(targetCellType: TargetCellType): RasterSource =
    GeoTiffResampleRasterSource(dataPath, resampleGrid, method, strategy, Some(targetCellType), Some(tiff))

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)

    read(bounds, bands)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)

    closestTiffOverview.synchronized { if (it.hasNext) Some(it.next) else None }
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val targetPixelBounds = extents.map(gridExtent.gridBoundsFor(_))
    // result extents may actually expand to cover pixels at our resolution
    // TODO: verify the logic here, should the sourcePixelBounds be calculated from input or expanded extent?
    readBounds(targetPixelBounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

    val windows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this.gridBounds)
    } yield {
      val targetExtent = gridExtent.extentFor(targetPixelBounds)
      val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetExtent, clamp = true)
      val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width.toInt, targetPixelBounds.height.toInt)
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
}
