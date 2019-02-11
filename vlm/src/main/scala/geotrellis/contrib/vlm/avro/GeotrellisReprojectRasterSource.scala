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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.spark._
import geotrellis.spark.io._

case class GeotrellisReprojectRasterSource(
  uri: String,
  baseLayerId: LayerId,
  bandCount: Int,
  crs: CRS,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[avro] val parentOptions: RasterViewOptions = RasterViewOptions()
) extends GeotrellisBaseRasterSource {
  def cellType: CellType = parentCellType
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  protected lazy val transform = Transform(parentCRS, crs)
  protected lazy val backTransform = Transform(crs, parentCRS)

  protected lazy val layerName = baseLayerId.name
  protected lazy val layerIds: Seq[LayerId] = GeotrellisRasterSource.getLayerIdsByName(reader, layerName)

  override lazy val rasterExtent: RasterExtent = reprojectOptions.targetRasterExtent match {
    case Some(targetRasterExtent) =>
      targetRasterExtent
    case None =>
      ReprojectRasterExtent(parentRasterExtent, transform, reprojectOptions)
  }

  private[avro] val options: RasterViewOptions = parentOptions.copy(crs = Some(crs), rasterExtent = Some(rasterExtent))

  override lazy val resolutions: List[RasterExtent] =
    GeotrellisRasterSource.getResolutions(reader, layerName).map(resolution =>
      ReprojectRasterExtent(resolution, transform))

  @transient lazy val layerId: LayerId = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(rasterExtent, backTransform, reprojectOptions)
      GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, estimatedSource.cellSize, strategy)
    } else {
      GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, baseRasterExtent.cellSize, strategy)
    }
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    for {
      _ <- rasterExtent.extent.intersection(extent)
      targetRasterExtent = rasterExtent.createAlignedRasterExtent(extent)
      sourceExtent = targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).envelope
      raster <- GeotrellisRasterSource.readIntersecting(reader, layerId, metadata, sourceExtent, bands)
    } yield {
      raster.reproject(targetRasterExtent, transform, backTransform, reprojectOptions).mapTile { _.convert(cellType) }
    }
  }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(extent => read(extent, bands))

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(bounds => read(bounds, bands))

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, crs, reprojectOptions, strategy, options)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val resampledReprojectOptions = reprojectOptions.copy(method = method, targetRasterExtent = Some(resampleGrid(rasterExtent)))
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, crs, resampledReprojectOptions, strategy, options)
  }

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTrellisConvertedRasterSource(uri, baseLayerId, cellType, bandCount, strategy, options)
}
