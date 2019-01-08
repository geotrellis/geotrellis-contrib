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
import geotrellis.contrib.vlm.geotiff._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.raster.{MultibandTile, Tile}

/**
  * Note: GeoTrellis AttributeStore does not store the band count for the layers by default,
  *       thus they need to be provided from application configuration.
  *
  * @param uri geotrellis catalog uri
  * @param layerId source layer from above catalog
  * @param bandCount number of bands for each tile in above layer
  */
case class GeotrellisRasterSource(uri: String, layerId: LayerId, bandCount: Int = 1) extends RasterSource {

  lazy val reader = CollectionLayerReader(uri)
  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
  lazy val rasterExtent: RasterExtent =
    metadata.layout.createAlignedGridExtent(metadata.extent).toRasterExtent()

  lazy val resolutions: List[RasterExtent] = reader.attributeStore.layerIds.map { currLayerId =>
    val layerMetadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](currLayerId)
    layerMetadata.layout.createAlignedGridExtent(layerMetadata.extent).toRasterExtent()
  }.toList

  def crs: CRS = metadata.crs

  def cellType: CellType = metadata.cellType

  def resampleMethod: Option[ResampleMethod] = None

  def readTiles(extent: Extent, bands: Seq[Int]): Seq[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
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

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val tiles = readTiles(extent, bands)
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

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    extents.toIterator.map(extent => read(extent, bands)).flatten
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds.toIterator.map(bounds => read(bounds, bands)).flatten
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    ???

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    ???
}
