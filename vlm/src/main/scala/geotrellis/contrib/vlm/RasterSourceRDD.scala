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

package geotrellis.contrib.vlm

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.partition._
import geotrellis.spark.tiling._
import geotrellis.proj4._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

object RasterSourceRDD {
  final val PARTITION_BYTES: Long = 128l * 1024 * 1024

  def read(
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition,
    partitionBytes: Long = PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val cellTypes = readingSources.map { _.source.cellType }.toSet
    require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = readingSources.map { _.source.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val combinedExtents = readingSources.map { _.source.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val tileSize = layout.tileCols * layout.tileRows * cellType.bytes

    val layerMetadata =
      TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)

    lazy val noDataTile = ArrayTile.alloc(cellType, layout.tileCols, layout.tileRows).fill(NODATA).interpretAs(cellType)

    val maxIndex = readingSources.map { _.targetBand }.max
    val targetIndexes: Seq[Int] = 0 to maxIndex

    val sourcesRDD: RDD[(SpatialKey, (Int, Option[MultibandTile]))] =
      sc.parallelize(readingSources).flatMap { source =>
        val layoutSource = new LayoutTileSource(source.source, layout)

        val sourceBand = source.sourceBand
        val targetBand = source.targetBand
        val keys = layoutSource.keys

        partition(keys, partitionBytes)( _ => tileSize).flatMap { _.map { key =>
          (key, (targetBand, layoutSource.read(key, Seq(sourceBand))))
          }
        }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size)
        sourcesRDD.repartition(count)
      else
        sourcesRDD
    }

    val groupedSourcesRDD: RDD[(SpatialKey, Iterable[(Int, Option[MultibandTile])])] =
      repartitioned.groupByKey()

    val result: RDD[(SpatialKey, MultibandTile)] =
    groupedSourcesRDD.mapValues { iter =>
      val mappedBands: Map[Int, Option[MultibandTile]] = iter.toSeq.sortBy { _._1 }.toMap

      val tiles: Seq[Tile] =
        targetIndexes.map { index =>
          mappedBands.get(index).getOrElse(None) match {
            case Some(multibandTile) => multibandTile.band(0)
            case None => noDataTile
          }
        }

      MultibandTile(tiles)
    }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def apply(
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    partitionBytes: Long = PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {

    val cellTypes = sources.map { _.cellType }.toSet
    require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = sources.map { _.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))

    val layerMetadata =
      TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)

    val sourcesRDD: RDD[(RasterSource, Array[SpatialKey])] =
      sc.parallelize(sources).flatMap { source =>
        val keys: Traversable[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              layout.mapTransform.keysForGeometry(intersection.toPolygon)
            case None =>
              Seq.empty[SpatialKey]
          }
        val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
        partition(keys, partitionBytes)( _ => tileSize).map { res => (source, res) }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size)
        sourcesRDD.repartition(count)
      else
        sourcesRDD
    }

    val result: RDD[(SpatialKey, MultibandTile)] =
      repartitioned.flatMap { case (source, keys) =>
        val tileSource = new LayoutTileSource(source, layout)
        tileSource.readAll(keys.toIterator)
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def apply(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply(Seq(source), layout)

  /** Partition a set of chunks not to exceed certain size per partition */
  private def partition[T: ClassTag](
    chunks: Traversable[T],
    maxPartitionSize: Long
  )(chunkSize: T => Long = { c: T => 1l }): Array[Array[T]] = {
    if (chunks.isEmpty) {
      Array[Array[T]]()
    } else {
      val partition = ArrayBuilder.make[T]
      partition.sizeHintBounded(128, chunks)
      var partitionSize: Long = 0l
      var partitionCount: Long = 0l
      val partitions = ArrayBuilder.make[Array[T]]

      def finalizePartition() {
        val res = partition.result
        if (res.nonEmpty) partitions += res
        partition.clear()
        partitionSize = 0l
        partitionCount = 0l
      }

      def addToPartition(chunk: T) {
        partition += chunk
        partitionSize += chunkSize(chunk)
        partitionCount += 1
      }

      for (chunk <- chunks) {
        if ((partitionCount == 0) || (partitionSize + chunkSize(chunk)) < maxPartitionSize)
          addToPartition(chunk)
        else {
          finalizePartition()
          addToPartition(chunk)
        }
      }

      finalizePartition()
      partitions.result
    }
  }
}
