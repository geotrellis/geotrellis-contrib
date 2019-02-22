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
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{Auto, AutoHigherResolution, Base, OverviewStrategy}
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._
import geotrellis.raster.{MultibandTile, Tile}

/**
  * Note: GeoTrellis AttributeStore does not store the band count for the layers by default,
  *       thus they need to be provided from application configuration.
  *
  * @param uri geotrellis catalog uri
  * @param layerId source layer from above catalog
  * @param bandCount number of bands for each tile in above layer
  */
case class GeotrellisRasterSource(
  uri: String,
  layerId: LayerId,
  bandCount: Int = 1,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource {

  lazy val reader = CollectionLayerReader(uri)
  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
  lazy val layerGridExtent: RasterExtent =
    metadata.layout.createAlignedGridExtent(metadata.extent).toRasterExtent()

  lazy val resolutions: List[RasterExtent] = GeotrellisRasterSource.getResolutions(reader, layerId.name)

  def crs: CRS = metadata.crs
  def cellType: CellType = dstCellType.getOrElse(metadata.cellType)
  def resampleMethod: Option[ResampleMethod] = None

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    GeotrellisRasterSource.read(reader, layerId, metadata, extent, bands).map { convertRaster }
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    extents.toIterator.flatMap(extent => read(extent, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds.toIterator.flatMap(bounds => read(bounds, bands))
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeotrellisReprojectRasterSource = {
    GeotrellisReprojectRasterSource(uri, layerId, bandCount, targetCRS, reprojectOptions, strategy, targetCellType)
  }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    GeotrellisResampleRasterSource(uri, layerId, bandCount, resampleGrid, method, strategy, targetCellType)
  }

  def convert(targetCellType: TargetCellType): RasterSource =
    GeotrellisRasterSource(uri, layerId, bandCount, Some(targetCellType))
}


object GeotrellisRasterSource {

  def getLayerIdsByName(reader: CollectionLayerReader[LayerId], layerName: String): Seq[LayerId] =
    reader.attributeStore.layerIds.filter(_.name == layerName)

  def getResolutions(reader: CollectionLayerReader[LayerId], layerName: String): List[RasterExtent] =
    getLayerIdsByName(reader, layerName)
      .map { currLayerId =>
        val layerMetadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](currLayerId)
        layerMetadata.layout.createAlignedGridExtent(layerMetadata.extent).toRasterExtent()
      }.toList

  def getClosestResolution(resolutions: List[RasterExtent], cellSize: CellSize, strategy: OverviewStrategy = AutoHigherResolution): Option[RasterExtent] = {
    strategy match {
      case AutoHigherResolution =>
        resolutions
          .map { v => (cellSize.resolution - v.cellSize.resolution) -> v }
          .filter(_._1 >= 0)
          .sortBy(_._1)
          .map(_._2)
          .headOption
      case Auto(n) =>
        resolutions
          .sortBy(v => math.abs(cellSize.resolution - v.cellSize.resolution))
          .lift(n) // n can be out of bounds,
      // makes only overview lookup as overview position is important
      case Base => None
    }
  }

  def getClosestLayer(resolutions: List[RasterExtent], layerIds: Seq[LayerId], baseLayerId: LayerId, cellSize: CellSize, strategy: OverviewStrategy = AutoHigherResolution): LayerId = {
    getClosestResolution(resolutions, cellSize, strategy) match {
      case Some(resolution) => {
        val resolutionLayerIds: Map[RasterExtent, LayerId] = (resolutions zip layerIds).toMap
        resolutionLayerIds.get(resolution) match {
          case Some(closestLayerId) => closestLayerId
          case None => baseLayerId
        }
      }
      case None => baseLayerId
    }
  }

  def readTiles(reader: CollectionLayerReader[LayerId], layerId: LayerId, extent: Extent, bands: Seq[Int]): Seq[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val header = reader.attributeStore.readHeader[LayerHeader](layerId)
    (header.keyClass, header.valueClass) match {
      case ("geotrellis.spark.SpatialKey", "geotrellis.raster.Tile") => {
        reader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          .where(Intersects(extent))
          .result
          .withContext(tiles =>
            // Convert single band tiles to multiband
            tiles.map{ case(key, tile) => (key, MultibandTile(tile)) }
          )
      }
      case ("geotrellis.spark.SpatialKey", "geotrellis.raster.MultibandTile") => {
        reader.query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
          .where(Intersects(extent))
          .result
          .withContext(tiles =>
            tiles.map{ case(key, tile) => (key, tile.subsetBands(bands)) }
          )
      }
      case _ => {
        throw new Exception("Unable to read single or multiband tiles from file")
      }
    }
  }

  def readIntersecting(reader: CollectionLayerReader[LayerId], layerId: LayerId, metadata: TileLayerMetadata[SpatialKey], extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val tiles = readTiles(reader, layerId, extent, bands)
    if (tiles.isEmpty)
      None
    else
      Some(tiles.stitch())
  }

  def read(reader: CollectionLayerReader[LayerId], layerId: LayerId, metadata: TileLayerMetadata[SpatialKey], extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val tiles = readTiles(reader, layerId, extent, bands)
    if (tiles.isEmpty)
      None
    else
      metadata.extent.intersection(extent) match {
        case Some(intersectionExtent) =>
          Some(tiles.stitch().crop(intersectionExtent))
        case None =>
          None
      }
  }
}
