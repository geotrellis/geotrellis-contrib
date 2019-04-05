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
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._

case class GeotrellisResampleRasterSource(
  uri: String,
  baseLayerId: LayerId,
  bandCount: Int,
  resampleGrid: ResampleGrid[Long],
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource { self =>
  lazy val reader = CollectionLayerReader(uri)

  lazy val baseMetadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](baseLayerId)
  lazy val baseGridExtent: GridExtent[Long] = baseMetadata.layout.createAlignedGridExtent(baseMetadata.extent)

  @transient protected lazy val layerId: LayerId =
    GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, gridExtent.cellSize, strategy)

  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

  def crs: CRS = metadata.crs
  def cellType: CellType = dstCellType.getOrElse(metadata.cellType)
  def resampleMethod: Option[ResampleMethod] = Some(method)

  lazy val layerName = baseLayerId.name
  lazy val gridExtent: GridExtent[Long] = resampleGrid(baseGridExtent)
  lazy val resolutions: List[GridExtent[Long]] = GeotrellisRasterSource.getResolutions(reader, layerName)
  lazy val layerIds: Seq[LayerId] = GeotrellisRasterSource.getLayerIdsByName(reader, layerName)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    GeotrellisRasterSource.readIntersecting(reader, layerId, metadata, extent, bands)
      .map { raster =>
        val targetRasterExtent = gridExtent.createAlignedRasterExtent(extent)
        raster.resample(targetRasterExtent, method)
      }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds
      .intersection(this.gridBounds)
      .map(gridExtent.extentFor(_).buffer(- cellSize.width / 2, - cellSize.height / 2))
      .flatMap(read(_, bands))
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeotrellisReprojectRasterSource =
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeotrellisResampleRasterSource(uri, baseLayerId, bandCount, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): RasterSource =
    GeotrellisResampleRasterSource(uri, baseLayerId, bandCount, resampleGrid, method, strategy, Some(targetCellType))

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(read(_, bands))

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(_.intersection(this.gridBounds).flatMap(read(_, bands)))
}
