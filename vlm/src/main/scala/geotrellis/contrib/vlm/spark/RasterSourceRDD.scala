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

package geotrellis.contrib.vlm.spark
import geotrellis.contrib.vlm.{LayoutTileSource, RasterRegion, RasterSource, ReadingSource}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.{ArrayTile, MultibandTile, NODATA, Tile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{ContextRDD, KeyBounds, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

object RasterSourceRDD {
  final val DEFAULT_PARTITION_BYTES: Long = 128l * 1024 * 1024

  def read(
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    read(readingSources, layout, DEFAULT_PARTITION_BYTES)

  def read(
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition,
    partitionBytes: Long
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

    def getNoDataTile = ArrayTile.alloc(cellType, layout.tileCols, layout.tileRows).fill(NODATA).interpretAs(cellType)

    val maxIndex = readingSources.map { _.sourceToTargetBand.values.max }.max
    val targetIndexes: Seq[Int] = 0 to maxIndex

    val sourcesRDD: RDD[(SpatialKey, (Int, Option[MultibandTile]))] =
      sc.parallelize(readingSources, readingSources.size).flatMap { source =>
        val layoutSource = new LayoutTileSource(source.source, layout)
        val keys = layoutSource.keys

        RasterSourceRDD.partition(keys, partitionBytes)( _ => tileSize).flatMap { _.flatMap { key =>
          source.sourceToTargetBand.map { case (sourceBand, targetBand) =>
            (key, (targetBand, layoutSource.read(key, Seq(sourceBand))))
          }
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
    groupedSourcesRDD.mapPartitions ({ partition =>
      val noDataTile = getNoDataTile

      partition.map { case (key, iter) =>
        val mappedBands: Map[Int, Option[MultibandTile]] = iter.toSeq.sortBy { _._1 }.toMap

        val tiles: Seq[Tile] =
          targetIndexes.map { index =>
            mappedBands.getOrElse(index, None) match {
              case Some(multibandTile) => multibandTile.band(0)
              case None => noDataTile
            }
          }

        key -> MultibandTile(tiles)
      }
    }, preservesPartitioning = true)

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def read(
    readingSourcesRDD: RDD[ReadingSource],
    layout: LayoutDefinition
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    read(readingSourcesRDD, layout, None)

  def read(
    readingSourcesRDD: RDD[ReadingSource],
    layout: LayoutDefinition,
    partitioner: Option[Partitioner]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val rasterSourcesRDD = readingSourcesRDD.map { _.source }
    val summary = RasterSummary.fromRDD(rasterSourcesRDD)

    val cellType = summary.cellType

    val layerMetadata: TileLayerMetadata[SpatialKey] = summary.toTileLayerMetadata(layout, 0)._1

    def getNoDataTile = ArrayTile.alloc(cellType, layout.tileCols, layout.tileRows).fill(NODATA).interpretAs(cellType)

    val maxIndex = readingSourcesRDD.map { _.sourceToTargetBand.values.max }.reduce { _ max _ }
    val targetIndexes: Seq[Int] = 0 to maxIndex

    val keyedRDD: RDD[(SpatialKey, (Int, Option[MultibandTile]))] =
      readingSourcesRDD.mapPartitions ({ partition =>
        partition.flatMap { source =>
          val layoutSource = LayoutTileSource(source.source, layout)

          layoutSource.keys.flatMap { key =>
            source.sourceToTargetBand.map { case (sourceBand, targetBand) =>
              (key, (targetBand, layoutSource.read(key, Seq(sourceBand))))
            }
          }
        }
      })

    val groupedRDD: RDD[(SpatialKey, Iterable[(Int, Option[MultibandTile])])] =
      keyedRDD.groupByKey(partitioner.getOrElse(SpatialPartitioner(summary.estimatePartitionsNumber)))

    val result: RDD[(SpatialKey, MultibandTile)] =
      groupedRDD.mapPartitions ({ partition =>
        val noDataTile = getNoDataTile

      partition.map { case (key, iter) =>
        val mappedBands: Map[Int, Option[MultibandTile]] = iter.toSeq.sortBy { _._1 }.toMap

        val tiles: Seq[Tile] =
          targetIndexes.map { index =>
            mappedBands.getOrElse(index, None) match {
              case Some(multibandTile) => multibandTile.band(0)
              case None => noDataTile
            }
          }

        key -> MultibandTile(tiles)
      }
    }, preservesPartitioning = true)

    ContextRDD(result, layerMetadata)
  }

  def tiledLayerRDD(
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    resampleMethod: ResampleMethod = NearestNeighbor,
    partitioner: Option[Partitioner] = None
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val summary = RasterSummary.fromRDD(sources)
    val layerMetadata = summary.toTileLayerMetadata(layout, 0)._1

    val tiledLayoutSourceRDD =
      sources.map { _.tileToLayout(layout, resampleMethod) }

    val rasterRegionRDD: RDD[(SpatialKey, RasterRegion)] =
      tiledLayoutSourceRDD.flatMap { _.keyedRasterRegions() }

    val tiledRDD: RDD[(SpatialKey, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner.getOrElse(SpatialPartitioner(summary.estimatePartitionsNumber)))
        .mapValues { iter =>
          MultibandTile(
            iter.flatMap { _.raster.toSeq.flatMap { _.tile.bands } }
          )
        }

    ContextRDD(tiledRDD, layerMetadata)
  }

  def apply(
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    partitionBytes: Long = DEFAULT_PARTITION_BYTES
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
