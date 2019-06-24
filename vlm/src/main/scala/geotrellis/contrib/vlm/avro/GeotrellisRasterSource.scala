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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{Auto, AutoHigherResolution, Base, OverviewStrategy}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{MultibandTile, Tile, _}
import geotrellis.spark.io._
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.vector._


case class Layer(id: LayerId, metadata: TileLayerMetadata[SpatialKey], bandCount: Int) {
  /** GridExtent of the data pixels in the layer */
  def gridExtent: GridExtent[Long] = metadata.layout.createAlignedGridExtent(metadata.extent)
}

/**
  * Note: GeoTrellis AttributeStore does not store the band count for the layers by default,
  *       thus they need to be provided from application configuration.
  *
  * @param dataPath geotrellis catalog DataPath
  * @param layerId source layer from above catalog
  * @param bandCount number of bands for each tile in above layer
  */
class GeotrellisRasterSource(
  val attributeStore: AttributeStore,
  val dataPath: GeoTrellisDataPath,
  val layerId: LayerId,
  val sourceLayers: Stream[Layer],
  val bandCount: Int,
  val targetCellType: Option[TargetCellType]
) extends RasterSource {

  def this(attributeStore: AttributeStore, dataPath: GeoTrellisDataPath, layerId: LayerId, bandCount: Int) =
    this(attributeStore, dataPath, layerId, GeotrellisRasterSource.getSouceLayersByName(attributeStore, layerId.name, bandCount), bandCount, None)

  def this(dataPath: GeoTrellisDataPath, layerId: LayerId, bandCount: Int) =
    this(AttributeStore(dataPath.catalogPath), dataPath, layerId, bandCount)

  def this(dataPath: GeoTrellisDataPath, layerId: LayerId) =
    this(AttributeStore(dataPath.catalogPath), dataPath, layerId, bandCount = 1)


  lazy val reader = CollectionLayerReader(attributeStore, dataPath.catalogPath)

  // read metadata directly instead of searching sourceLayers to avoid unneeded reads
  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

  lazy val gridExtent: GridExtent[Long] = metadata.layout.createAlignedGridExtent(metadata.extent)

  def crs: CRS = metadata.crs

  def cellType: CellType = dstCellType.getOrElse(metadata.cellType)

  // reference to this will fully initilze the sourceLayers stream
  lazy val resolutions: List[GridExtent[Long]] = sourceLayers.map(_.gridExtent).toList

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    GeotrellisRasterSource.read(reader, layerId, metadata, extent, bands).map { convertRaster }
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds
      .intersection(this.gridBounds)
      .map(gridExtent.extentFor(_).buffer(- cellSize.width / 2, - cellSize.height / 2))
      .flatMap(read(_, bands))
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(read(_, bands))

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(_.intersection(this.gridBounds).flatMap(read(_, bands)))

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource = {
    if (targetCRS != this.crs) {
      val (closestLayerId, targetGridExtent) = GeotrellisReprojectRasterSource.getClosestSourceLayer(targetCRS, sourceLayers, reprojectOptions, strategy)
      new GeotrellisReprojectRasterSource(attributeStore, dataPath, closestLayerId, sourceLayers, targetGridExtent, targetCRS, reprojectOptions, targetCellType)
    } else {
      // TODO: add unit tests for this in particular, the behavior feels murky
      ResampleGrid.fromReprojectOptions(reprojectOptions) match {
        case Some(resampleGrid) =>
          val resampledGridExtent = resampleGrid(this.gridExtent)
          val closestLayerId = GeotrellisRasterSource.getClosestResolution(sourceLayers.toList, resampledGridExtent.cellSize, strategy)(_.metadata.layout.cellSize).get.id
          new GeotrellisResampleRasterSource(attributeStore, dataPath, closestLayerId, sourceLayers, resampledGridExtent, reprojectOptions.method, targetCellType)
        case None =>
          this // I think I was asked to do nothing
      }
    }
  }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val resampledGridExtent = resampleGrid(this.gridExtent)
    val closestLayerId = GeotrellisRasterSource.getClosestResolution(sourceLayers.toList, resampledGridExtent.cellSize, strategy)(_.metadata.layout.cellSize).get.id
    new GeotrellisResampleRasterSource(attributeStore, dataPath, closestLayerId, sourceLayers, resampledGridExtent, method, targetCellType)
  }

  def convert(targetCellType: TargetCellType): RasterSource =
    new GeotrellisRasterSource(attributeStore, dataPath, layerId, sourceLayers, bandCount, Some(targetCellType))

  override def toString: String =
    s"GeoTrellisRasterSource($dataPath, $layerId)"
}


object GeotrellisRasterSource {
  def getClosestResolution[T](
    grids: Seq[T],
    cellSize: CellSize,
    strategy: OverviewStrategy = AutoHigherResolution
  )(implicit f: T => CellSize): Option[T] = {
    val maxResultion = Some(grids.minBy(g => f(g).resolution))

    strategy match {
      case AutoHigherResolution =>
        grids // overviews can have erased extent information
          .map { v => (cellSize.resolution - f(v).resolution) -> v }
          .filter(_._1 >= 0)
          .sortBy(_._1)
          .map(_._2)
          .headOption
          .orElse(maxResultion)
      case Auto(n) =>
        val sorted = grids.sortBy(v => math.abs(cellSize.resolution - f(v).resolution))
        sorted.lift(n).orElse(sorted.lastOption) // n can be out of bounds,
      // makes only overview lookup as overview position is important
      case Base => maxResultion
    }
  }

  /** Read metadata for all layers that share a name and sort them by their resolution */
  def getSourceLayersByName(attributeStore: AttributeStore, layerName: String, bandCount: Int): Stream[Layer] = {
    attributeStore.
      layerIds.
      filter(_.name == layerName).
      sortWith(_.zoom > _.zoom).
      toStream. // We will be lazy about fetching higher zoom levels
      map { id =>
        val metadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](id)
        Layer(id, metadata, bandCount)
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
    sparseStitch(tiles, extent)
  }

  def read(reader: CollectionLayerReader[LayerId], layerId: LayerId, metadata: TileLayerMetadata[SpatialKey], extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val tiles = readTiles(reader, layerId, extent, bands)
    metadata.extent.intersection(extent) flatMap { intersectionExtent =>
      sparseStitch(tiles, intersectionExtent).map(_.crop(intersectionExtent))
    }
  }

  /**
   *  The stitch method in gtcore is unable to handle missing spatialkeys correctly.
   *  This method works around that problem by attempting to infer any missing tiles
   **/
  def sparseStitch(
    tiles: Seq[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    extent: Extent
  ): Option[Raster[MultibandTile]] = {
    val md = tiles.metadata
    val expectedKeys = md
      .mapTransform(extent)
      .coordsIter
      .map { case (x, y) => SpatialKey(x, y) }
      .toList
    val actualKeys = tiles.map(_._1)
    val missingKeys = expectedKeys diff actualKeys

    val missingTiles = missingKeys.map { key =>
      (key, MultibandTile(ArrayTile.empty(md.cellType, md.tileLayout.tileCols, md.tileLayout.tileRows)))
    }
    val allTiles = tiles.withContext { collection =>
      collection.toList ::: missingTiles
    }
    if (allTiles.isEmpty) None
    else Some(allTiles.stitch())
  }
}
