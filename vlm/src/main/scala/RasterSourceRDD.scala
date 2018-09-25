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
    //val sourcesRDD: RDD[(RasterSource, Array[GridBounds])] =
    val sourcesRDD: RDD[(RasterSource, Array[PaddedTile])] =
      sc.parallelize(sources).flatMap { source =>
        //val extents: Traversable[Extent] =
        //val extents: Traversable[GridBounds] =
        val extents: Traversable[PaddedTile] =
        //val extents: Map[GridBounds, GridBounds] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              val keys =
                mapTransform
                  .keysForGeometry(intersection.toPolygon)

              val intersectionGridBounds: GridBounds =
                rasterExtent.gridBoundsFor(intersection)

              val keyGridBounds =
                keys
                  .map { key => rasterExtent.gridBoundsFor(mapTransform(key)) }

              val combinedGridBounds = keyGridBounds.reduce { _ combine _ }

              println(s"\nThis is the combinedGridBounds: $combinedGridBounds")
              println(s"This is the intersectionGridBounds: $intersectionGridBounds")

              val adjustedIntersectionGridBounds = {
                val colMin = intersectionGridBounds.colMin - combinedGridBounds.colMin
                val rowMin = intersectionGridBounds.rowMin - combinedGridBounds.rowMin

                GridBounds(
                  colMin,
                  rowMin,
                  source.cols + colMin - 1,
                  source.rows + rowMin - 1
                )
              }

              println(s"This is the adjustedIntersectionGridBounds: $adjustedIntersectionGridBounds")

              /*
              println(s"\nThis is the gridBounds of the intersection: ${intersectionGridBounds}")
              println(s"This is the gridBounds of the intersection: ${intersectionGridBounds}")
              println(s"This is the width of the intersection: ${intersectionGridBounds.width}")
              println(s"This is the height of the intersection: ${intersectionGridBounds.height}")
              println(s"\nThis is the adjustedInterGridBounds: ${adjustedIntersectionGridBounds}")
              println(s"This is the adjustedInterGridBounds' width: ${adjustedIntersectionGridBounds.width}")
              println(s"This is the adjustedInterGridBounds' height: ${adjustedIntersectionGridBounds.height}")
              */

              keyGridBounds.map { gb =>
                val adjusted = gb.offset(-combinedGridBounds.colMin, -combinedGridBounds.rowMin)

                println(s"This is the adjusted key gridBounds: $adjusted")

                val result =
                  adjusted.intersection(adjustedIntersectionGridBounds).get

                PaddedTile(result, adjusted, mapTransform.boundsToExtent(gb), adjustedIntersectionGridBounds.colMin, adjustedIntersectionGridBounds.rowMin)
              }


              //adjustedKeyGridBounds.split(256, 256).toSeq

              /*
              val (colMin, rowMin) = {
                val keyExtent = mapTransform(keys.head)

                rasterExtent.mapToGrid(keyExtent.xmin, keyExtent.ymax)
              }

              val (colMax, rowMax) = {
                val keyExtent = mapTransform(keys(keys.size - 1))

                rasterExtent.mapToGrid(keyExtent.xmax, keyExtent.ymin)
              }

              GridBounds(0, 0, colMax - colMin, rowMax - rowMin)
                .split(layout.tileCols, layout.tileRows)
                .toSeq
              */

              //keys.map { key => mapTransform(key) }
            //case None => Seq.empty[Extent]
            //case None => Seq.empty[GridBounds]
            case None => Seq.empty[PaddedTile]
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
        //source.readBounds(extents).map { raster =>
        source.readPaddedTiles(extents).map { raster =>
          val center = raster.extent.center
          val key = mapTransform.pointToKey(center)
          //println(s"Tile.cols: ${raster.tile.cols} Tile.rows: ${raster.tile.rows}")
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
