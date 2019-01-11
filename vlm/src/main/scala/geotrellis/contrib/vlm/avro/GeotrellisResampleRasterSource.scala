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
import geotrellis.contrib.vlm.avro._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.io.geotiff.{Auto, AutoHigherResolution, Base, OverviewStrategy}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._

case class GeotrellisResampleRasterSource(
  uri: String,
  layerName: String,
  bandCount: Int,
  baseRasterExtent: RasterExtent,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution
) extends RasterSource { self =>
  lazy val reader = CollectionLayerReader(uri)

  @transient protected lazy val layerId: LayerId =
    getClosestLayer(rasterExtent.cellSize, strategy)

  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

  def crs: CRS = metadata.crs
  def cellType: CellType = metadata.cellType
  def resampleMethod: Option[ResampleMethod] = Some(method)

  lazy val rasterExtent: RasterExtent = resampleGrid(baseRasterExtent)
  lazy val resolutions: List[RasterExtent] = GeotrellisRasterSource.getResolutions(reader, layerName)
  lazy val layerIds: Seq[LayerId] = GeotrellisRasterSource.getLayerIdsByName(reader, layerName)
  lazy val resolutionLayerIds: Map[RasterExtent, LayerId] = (resolutions zip layerIds).toMap

  def getClosestLayer(cellSize: CellSize, strategy: OverviewStrategy = AutoHigherResolution): LayerId = {
    val closestResolution = strategy match {
      case AutoHigherResolution =>
        resolutions
          .map { v => (cellSize.resolution - v.cellSize.resolution) -> v }
          .filter(_._1 >= 0)
          .sortBy(_._1)
          .map(_._2)
          .headOption
          .getOrElse(rasterExtent)
      case Auto(n) =>
        resolutions
          .sortBy(v => math.abs(cellSize.resolution - v.cellSize.resolution))
          .lift(n)
          .getOrElse(rasterExtent) // n can be out of bounds,
      // makes only overview lookup as overview position is important
      case Base => rasterExtent
    }
    resolutionLayerIds.get(closestResolution) match {
      case Some(closestLayerId) =>
        closestLayerId
      case None =>
        throw new Exception("Could not find closest layer")
    }
  }

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    GeotrellisRasterSource.read(reader, layerId, metadata, extent, bands)
      .map { raster =>
        val targetRasterExtent = rasterExtent.createAlignedRasterExtent(extent)
        raster.resample(targetRasterExtent, method)
      }
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    ???

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeotrellisResampleRasterSource(uri, layerName, bandCount, baseRasterExtent, resampleGrid, method, strategy)

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(extent => read(extent, bands))

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(bounds => read(bounds, bands))
}
