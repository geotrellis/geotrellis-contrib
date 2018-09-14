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

    val sourcesRDD: RDD[(RasterSource, Array[RasterExtent])] =
      sc.parallelize(sources).flatMap { source =>
        val rasterExtents: Traversable[RasterExtent] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              val keys = mapTransform.keysForGeometry(intersection.toPolygon)

              keys.map { key => RasterExtent(mapTransform(key), layout.tileCols, layout.tileRows) }
            case None => Seq.empty[RasterExtent]
          }

        RasterExtentPartitioner
          .partitionRasterExtents(rasterExtents, partitionBytes)
          .map { res => (source, res) }
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
      repartitioned.flatMap { case (source, rasterExtents) =>
        source.read(rasterExtents).map { raster =>
          val center = raster.extent.center
          val key = mapTransform.pointToKey(center)

          (key, raster.tile)
        }
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def apply(source: RasterSource, layout: LayoutDefinition)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] =
    apply(Seq(source), layout)
}
