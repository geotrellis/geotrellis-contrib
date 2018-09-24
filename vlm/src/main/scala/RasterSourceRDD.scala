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

import spire.syntax.cfor._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag

object RasterSourceRDD {
  final val PARTITION_BYTES: Long = 128l * 1024 * 1024

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

    val rasterExtent =
      RasterExtent(
        layout.extent,
        layout.cellwidth,
        layout.cellheight,
        layout.layoutCols * layout.tileCols,
        layout.layoutRows * layout.tileRows
      )

    //val sourcesRDD: RDD[(RasterSource, Array[Extent])] =
    val sourcesRDD: RDD[(RasterSource, Array[GridBounds])] =
      sc.parallelize(sources).flatMap { source =>
        //val extents: Traversable[Extent] =
        val extents: Traversable[GridBounds] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              val keys =
                mapTransform
                  .keysForGeometry(intersection.toPolygon)
                  .toSeq
                  .sortBy { key => (key.col, key.row) }

              val (colMin, rowMin) = {
                val keyExtent = mapTransform(keys.head)

                rasterExtent.mapToGrid(keyExtent.xmin, keyExtent.ymin)
              }

              val (colMax, rowMax) = {
                val keyExtent = mapTransform(keys(keys.size - 1))

                rasterExtent.mapToGrid(keyExtent.xmax, keyExtent.ymax)
              }

              val targetColMax = colMax - colMin
              val targetRowMax = rowMax - rowMin

              println(s"\nThis is the targetColMax: $targetColMax")
              println(s"This is the targetRowMax: $targetRowMax")

              for {
                cols <- 0 until targetColMax + 1 by layout.tileCols
                rows <- 0 until targetRowMax + 1 by layout.tileRows
              } yield {
                GridBounds(
                  cols,
                  rows,
                  math.min(cols + layout.tileCols - 1, targetColMax - 1),
                  math.min(rows + layout.tileRows - 1, targetRowMax - 1)
                )
              }

              //GridBounds(0, 0, colMax - colMin, rowMax - rowMin)
                //.split(layout.tileCols, layout.tileRows)
                //.filter { gb => gb.width != 1 && gb.height != 1 }
                //.toList

              /*
              keys.map { key =>
                val ex = mapTransform(key)

                val (colMin, rowMin) = rasterExtent.mapToGrid(ex.xmin, ex.ymax)
                val (colMax, rowMax) = rasterExtent.mapToGrid(ex.xmax, ex.ymin)

                val gridBounds  = mapTransform(ex)
                val gridBounds2 = rasterExtent.gridBoundsFor(ex)

                /*
                println(s"This is the gridBounds for $key")
                println(s"This is the gridBounds produced by mapTransform: $gridBounds")
                println(s"This is the gridBound's width: ${gridBounds.width}")
                println(s"This is the girdBound's height: ${gridBounds.height}")
                println(s"This is the gridBounds produced by rasterExtent: $gridBounds2")
                println(s"This is the gridBound's width: ${gridBounds2.width}")
                println(s"This is the girdBound's height: ${gridBounds2.height}")
                */

                //gridBounds2
              }
              */
            //case None => Seq.empty[Extent]
            case None => Seq.empty[GridBounds]
          }

        partition(extents, partitionBytes).map { res => (source, res) }
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
      repartitioned.flatMap { case (source, extents) =>
        //source.readExtents(extents).map { raster =>
        source.readBounds(extents).map { raster =>
          val center = raster.extent.center
          val key = mapTransform.pointToKey(center)
          require(raster.tile.cols == layout.tileCols && raster.tile.rows == layout.tileRows)
          (key, raster.tile)
        }
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def apply(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply(Seq(source), layout)

  /** Partition a set of chunks not to exceed certain size per partition */
  private def partition[T: ClassTag](
    chunks: Traversable[T],
    maxPartitionSize: Long,
    chunkSize: T => Long = { c: T => 1l }
  ): Array[Array[T]] = {
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
