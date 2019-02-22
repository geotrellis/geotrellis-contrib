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
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource { self =>
  lazy val reader = CollectionLayerReader(uri)

  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

  def cellType: CellType = dstCellType.getOrElse(metadata.cellType)
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  protected lazy val baseCRS: CRS = baseMetadata.crs
  protected lazy val baseMetadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](baseLayerId)
  protected lazy val baseRasterExtent: RasterExtent =
    baseMetadata.layout.createAlignedGridExtent(baseMetadata.extent).toRasterExtent()

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)

  protected lazy val layerName = baseLayerId.name
  protected lazy val layerIds: Seq[LayerId] = GeotrellisRasterSource.getLayerIdsByName(reader, layerName)

  override lazy val layerGridExtent: RasterExtent = reprojectOptions.targetRasterExtent match {
    case Some(targetRasterExtent) =>
      targetRasterExtent
    case None =>
      ReprojectRasterExtent(baseRasterExtent, transform, reprojectOptions)
  }

  lazy val resolutions: List[RasterExtent] =
    GeotrellisRasterSource.getResolutions(reader, layerName).map(resolution =>
      ReprojectRasterExtent(resolution, transform))

  @transient private[vlm] lazy val layerId: LayerId = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(layerGridExtent, backTransform, reprojectOptions)
      GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, estimatedSource.cellSize, strategy)
    } else {
      GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, baseRasterExtent.cellSize, strategy)
    }
  }

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    for {
      _ <- this.extent.intersection(extent)
      targetRasterExtent = layerGridExtent.createAlignedRasterExtent(extent)
      sourceExtent = targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).envelope
      raster <- GeotrellisRasterSource.readIntersecting(reader, layerId, metadata, sourceExtent, bands)
    } yield {
      convertRaster(raster.reproject(targetRasterExtent, transform, backTransform, reprojectOptions))
    }
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(extent => read(extent, bands))

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(bounds => read(bounds, bands))

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val resampledReprojectOptions = reprojectOptions.copy(method = method, targetRasterExtent = Some(resampleGrid(self.layerGridExtent)))
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, crs, resampledReprojectOptions, strategy)
  }

  def convert(targetCellType: TargetCellType): RasterSource =
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, crs, reprojectOptions, strategy, Some(targetCellType))
}
