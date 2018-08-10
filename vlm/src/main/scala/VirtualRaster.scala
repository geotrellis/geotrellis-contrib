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

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.proj4._
import geotrellis.util._

import cats.effect.IO

import org.apache.spark._
import org.apache.spark.rdd._


class VirtualRaster[T](
  readers: Seq[RasterReader2[T]],
  layout: LayoutDefinition,
  targetCRS: CRS, // the target CRS for all outputted tiles.
  partitionBytes: Long = VirtualRaster.DEFAULT_PARTITION_BYTES,
  reprojectOptions: Reproject.Options
) extends Serializable {

  val mapTransform = layout.mapTransform

  def extent: Extent = layout.extent
  def cols: Int = layout.tileCols
  def rows: Int = layout.tileRows

  private def numPartitions = readers.size

  private def getRasterExtents(
    reader: RasterReader2[T],
    targetExtent: Option[Extent]
  ): Traversable[RasterExtent] = {
    val regionExtent = targetExtent.getOrElse(extent)
    val transform = Transform(reader.crs, targetCRS)
    val footprint: Extent = ReprojectRasterExtent.reprojectExtent(reader.rasterExtent, transform)

    regionExtent.intersection(footprint) match {
      case Some(intersection) =>
        val keys = mapTransform.keysForGeometry(intersection.toPolygon)

          keys.map { key =>
            val keyExtent = mapTransform.keyToExtent(key)
            val keyGridBounds = mapTransform.extentToBounds(keyExtent)

            RasterExtent(keyExtent, keyGridBounds.width, keyGridBounds.height)
          }
      case None => Array[RasterExtent]()
    }
  }

  private def readInRDD(
    targetCellType: CellType,
    targetExtent: Option[Extent]
  )(implicit sc: SparkContext): RDD[(SpatialKey, Raster[MultibandTile])] = {
    val readersRDD: RDD[(RasterReader2[T], Array[RasterExtent])] =
      sc.parallelize(readers, numPartitions)
        .flatMap { reader =>
          val rasterExtents = getRasterExtents(reader, targetExtent)

          val totalRasterExtents = rasterExtents.size * reader.bandCount

          val maxPartitionBytes =
            partitionBytes / math.max(targetCellType.bytes * totalRasterExtents, totalRasterExtents)

          RasterExtentPartitioner.partitionRasterExtents(rasterExtents, maxPartitionBytes).map { res =>
            (reader, res)
          }
        }

    readersRDD.persist()

    val repartitioned = {
      val count = readersRDD.count.toInt
      if (count > readersRDD.partitions.size)
        readersRDD.repartition(count)
      else
        readersRDD
    }

    val result =
      repartitioned.flatMap { case (reader, rasterExtents) =>
        val rasters = reader.read(rasterExtents, targetCRS, targetCellType, reprojectOptions)

        rasters.map { raster =>
          val center = raster.extent.center
          val k = mapTransform.pointToKey(center)
          (k, raster)
        }
      }

    readersRDD.unpersist()
    result
  }

  def getKeysRDD(
    keys: Seq[SpatialKey],
    targetCellType: CellType
  )(implicit sc: SparkContext): RDD[(SpatialKey, Raster[MultibandTile])] = {
    val sortedKeys: Seq[SpatialKey] = keys.sortBy { key => (key.col, key.row) }

    // Determine the Extent that the keys cover
    val keysExtent: Extent =
      mapTransform(sortedKeys.head)
        .combine(mapTransform(sortedKeys(sortedKeys.size - 1)))

    readInRDD(targetCellType, Some(keysExtent))
  }

  def readRDD(targetCellType: CellType)(implicit sc: SparkContext): RDD[(SpatialKey, Raster[MultibandTile])] =
    readInRDD(targetCellType, None)

  def readKey(key: SpatialKey): Iterator[T] = ???

  def read(windows: Traversable[RasterExtent]): Iterator[T] = {
    ???
  }

  def asCRS(crs: CRS, cellSize: CellSize): VirtualRaster[T] = ??? // same data in different CRS
}

object VirtualRaster {
  final val DEFAULT_PARTITION_BYTES: Long = 128l * 1024 * 1024

  def apply[T](
    readers: Seq[RasterReader2[T]],
    layout: LayoutDefinition,
    targetCRS: CRS,
    partitionBytes: Long,
    reprojectOptions: Reproject.Options
  ): VirtualRaster[T] =
    new VirtualRaster[T](readers, layout, targetCRS, partitionBytes, reprojectOptions)

  def apply[T](
    readers: Seq[RasterReader2[T]],
    layout: LayoutDefinition,
    targetCRS: CRS
  ): VirtualRaster[T] =
    new VirtualRaster[T](readers, layout, targetCRS, reprojectOptions = Reproject.Options.DEFAULT)

  def apply[T](
    readers: Seq[RasterReader2[T]],
    layout: LayoutDefinition
  ): VirtualRaster[T] = {
    val crss = readers.map { _.crs }.toSet

    if (crss.size > 1)
      throw new Exception(s"The given RaterReaders have multiple CRS's: $crss, but they must have only one")
    else
      apply(readers, layout, crss.head)
  }

  case class Source[T](reader: RasterReader2[T], footprint: Polygon)
  object Source {
    def apply[T](reader: RasterReader2[T], target: CRS): Source[T] = ???
  }
}
