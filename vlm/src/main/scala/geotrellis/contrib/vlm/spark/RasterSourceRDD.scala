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

package geotrellis.contrib.vlm.spark

import geotrellis.contrib.vlm._
import geotrellis.raster.resample._
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.util._

import org.apache.spark.rdd._
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
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    read(readingSources, layout, keyTransform, DEFAULT_PARTITION_BYTES)

  def read(
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition,
    partitionBytes: Long
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    readPartitionBytes[SpatialKey](readingSources, layout, (_, sk) => sk, partitionBytes)

  def read(
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey,
    partitionBytes: Long
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    readPartitionBytes[SpaceTimeKey](readingSources, layout, keyTransform, partitionBytes)

  def readPartitionBytes[K: SpatialComponent: TileToLayout: Boundable: ClassTag](
    readingSources: Seq[ReadingSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => K,
    partitionBytes: Long
  )(implicit sc: SparkContext): MultibandTileLayerRDD[K] = {
    val summary = RasterSummary.fromSeq(readingSources.map(_.source), keyTransform)
    val cellType = summary.cellType
    val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
    val layerMetadata = summary.toTileLayerMetadata(layout, 0)._1

    def getNoDataTile = ArrayTile.alloc(cellType, layout.tileCols, layout.tileRows).fill(NODATA).interpretAs(cellType)

    val maxIndex = readingSources.map { _.sourceToTargetBand.values.max }.max
    val targetIndexes: Seq[Int] = 0 to maxIndex

    val sourcesRDD: RDD[(K, (Int, Option[MultibandTile]))] =
      sc.parallelize(readingSources, readingSources.size).flatMap { source =>
        val layoutSource = new LayoutTileSource(source.source, layout, keyTransform)
        val keys = layoutSource.keys

        RasterSourceRDD.partition(keys, partitionBytes)( _ => tileSize).flatMap { _.flatMap { key =>
          source.sourceToTargetBand.map { case (sourceBand, targetBand) =>
            (key, (targetBand, layoutSource.read(key, Seq(sourceBand))))
          }
        } }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size) sourcesRDD.repartition(count)
      else sourcesRDD
    }

    val groupedSourcesRDD: RDD[(K, Iterable[(Int, Option[MultibandTile])])] =
      repartitioned.groupByKey()

    val result: RDD[(K, MultibandTile)] =
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
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    read[SpatialKey](readingSourcesRDD, layout, (_, sk) => sk, partitioner)

  def read(
    readingSourcesRDD: RDD[ReadingSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    read(readingSourcesRDD, layout, keyTransform, None)

  def read(
    readingSourcesRDD: RDD[ReadingSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey,
    partitioner: Option[Partitioner]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    read[SpaceTimeKey](readingSourcesRDD, layout, keyTransform, partitioner)

  def read[K: SpatialComponent: Boundable: TileToLayout: ClassTag](
    readingSourcesRDD: RDD[ReadingSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => K,
    partitioner: Option[Partitioner]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[K] = {
    val rasterSourcesRDD = readingSourcesRDD.map { _.source }
    val summary = RasterSummary.fromRDD(rasterSourcesRDD, keyTransform)
    val cellType = summary.cellType
    val layerMetadata = summary.toTileLayerMetadata(layout, 0)._1

    def getNoDataTile = ArrayTile.alloc(cellType, layout.tileCols, layout.tileRows).fill(NODATA).interpretAs(cellType)

    val maxIndex = readingSourcesRDD.map { _.sourceToTargetBand.values.max }.reduce { _ max _ }
    val targetIndexes: Seq[Int] = 0 to maxIndex

    val keyedRDD: RDD[(K, (Int, Option[MultibandTile]))] =
      readingSourcesRDD.mapPartitions ({ partition =>
        partition.flatMap { source =>
          val layoutSource = new LayoutTileSource[K](source.source, layout, keyTransform)

          layoutSource.keys.flatMap { key =>
            source.sourceToTargetBand.map { case (sourceBand, targetBand) =>
              (key, (targetBand, layoutSource.read(key, Seq(sourceBand))))
            }
          }
        }
      })

    val groupedRDD: RDD[(K, Iterable[(Int, Option[MultibandTile])])] = {
      // The number of partitions estimated by RasterSummary can sometimes be much
      // lower than what the user set. Therefore, we assume that the larger value
      // is the optimal number of partitions to use.
      val partitionCount =
      math.max(keyedRDD.getNumPartitions, summary.estimatePartitionsNumber)

      keyedRDD.groupByKey(partitioner.getOrElse(SpatialPartitioner[K](partitionCount)))
    }

    val result: RDD[(K, MultibandTile)] =
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

  def tiledLayerRDD(sources: RDD[RasterSource], layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    tiledLayerRDD(sources, layout, (_, sk) => sk, NearestNeighbor, None, None)

  def tiledLayerRDD(
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    resampleMethod: ResampleMethod,
    rasterSummary: Option[RasterSummary[SpatialKey]],
    partitioner: Option[Partitioner]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    tiledLayerRDD[SpatialKey](sources, layout, (_, sk) => sk, resampleMethod, rasterSummary, partitioner)

  def tiledLayerRDD(
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    tiledLayerRDD[SpaceTimeKey](sources, layout, keyTransform, NearestNeighbor, None, None)

  def tiledLayerRDD(
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey,
    resampleMethod: ResampleMethod,
    rasterSummary: Option[RasterSummary[SpaceTimeKey]],
    partitioner: Option[Partitioner]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    tiledLayerRDD[SpaceTimeKey](sources, layout, keyTransform, resampleMethod, rasterSummary, partitioner)

  def tiledLayerRDD[K: SpatialComponent: Boundable: TileToLayout: ClassTag](
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => K,
    resampleMethod: ResampleMethod = NearestNeighbor,
    rasterSummary: Option[RasterSummary[K]] = None,
    partitioner: Option[Partitioner] = None
  )(implicit sc: SparkContext): MultibandTileLayerRDD[K] = {
    val summary = rasterSummary.getOrElse(RasterSummary.fromRDD(sources, keyTransform))
    val layerMetadata = summary.toTileLayerMetadata(layout, 0)._1

    val tiledLayoutSourceRDD =
      sources.map { _.tileToLayout(layout, keyTransform, resampleMethod) }

    val rasterRegionRDD: RDD[(K, RasterRegion)] =
      tiledLayoutSourceRDD.flatMap { _.keyedRasterRegions() }

    // The number of partitions estimated by RasterSummary can sometimes be much
    // lower than what the user set. Therefore, we assume that the larger value
    // is the optimal number of partitions to use.
    val partitionCount =
      math.max(rasterRegionRDD.getNumPartitions, summary.estimatePartitionsNumber)

    val tiledRDD: RDD[(K, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner.getOrElse(SpatialPartitioner[K](partitionCount)))
        .mapValues { iter =>
          MultibandTile(
            iter.flatMap { _.raster.toSeq.flatMap { _.tile.bands } }
          )
        }

    ContextRDD(tiledRDD, layerMetadata)
  }

  def spatial(
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    partitionBytes: Long = DEFAULT_PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply[SpatialKey](sources, layout, (_, sk) => sk, partitionBytes)

  def spatial(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    spatial(Seq(source), layout)

  def temporal(
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey,
    partitionBytes: Long = DEFAULT_PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    apply[SpaceTimeKey](sources, layout, keyTransform, partitionBytes)

  def temporal(source: RasterSource, layout: LayoutDefinition, keyTransform: (RasterSource, SpatialKey) => SpaceTimeKey)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    temporal(Seq(source), layout, keyTransform)

  def apply[K: SpatialComponent: Boundable: TileToLayout](
    sources: Seq[RasterSource],
    layout: LayoutDefinition,
    keyTransform: (RasterSource, SpatialKey) => K,
    partitionBytes: Long = DEFAULT_PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[K] = {
    val summary = RasterSummary.fromSeq(sources, keyTransform)
    val extent = summary.extent
    val cellType = summary.cellType
    val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
    val layerMetadata = summary.toTileLayerMetadata(layout, 0)._1

    val sourcesRDD: RDD[(RasterSource, Array[SpatialKey])] =
      sc.parallelize(sources).flatMap { source =>
        val keys: Traversable[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) => layout.mapTransform.keysForGeometry(intersection.toPolygon)
            case None => Seq.empty[SpatialKey]
          }
        partition(keys, partitionBytes)( _ => tileSize).map { res => (source, res) }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size) sourcesRDD.repartition(count)
      else sourcesRDD
    }

    val result: RDD[(K, MultibandTile)] =
      repartitioned.flatMap { case (source, keys) =>
        val keysK = keys.map(keyTransform(source, _))
        val tileSource = new LayoutTileSource(source, layout, keyTransform)
        tileSource.readAll(keysK.toIterator)
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

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
