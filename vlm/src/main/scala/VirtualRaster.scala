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
import geotrellis.vector.reproject.{Reproject => VectorReproject}
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
  reprojectOptions: Reproject.Options
) extends Serializable {

  val mapTransform = layout.mapTransform

  def extent: Extent = layout.extent
  def cols: Int = layout.tileCols
  def rows: Int = layout.tileRows

  def numPartitions = readers.size

  def getKeysRDD(
    keys: Seq[SpatialKey],
    targetCellType: CellType
  )(implicit sc: SparkContext): RDD[(SpatialKey, Raster[MultibandTile])] = {
    val sortedKeys: Seq[SpatialKey] = keys.sortBy { key => (key.col, key.row) }

    // Determine the Extent that the keys cover
    val keysExtent: Extent =
      mapTransform(sortedKeys.head)
        .combine(mapTransform(sortedKeys(sortedKeys.size - 1)))

    val keyGridBounds = mapTransform.extentToBounds(keysExtent)
    val targetCols = keyGridBounds.width
    val targetRows = keyGridBounds.height

    sc.parallelize(readers, numPartitions)
      .flatMap { reader =>
        val backTransform = Transform(reader.crs, targetCRS)
        val footprint: Extent = ReprojectRasterExtent.reprojectExtent(reader.rasterExtent, backTransform)

        keysExtent.intersection(footprint) match {
          case Some(intersection) =>
            val keys = mapTransform.keysForGeometry(intersection.toPolygon())

            val rasterExtents = keys.map { key => RasterExtent(mapTransform(key), targetCols, targetRows) }
            val rasters = reader.read(rasterExtents, targetCRS, reprojectOptions)

            rasters.map { raster =>
              val center = raster.extent.center
              val k = mapTransform.pointToKey(center)
              (k, raster)
            }
          case None => Seq()
        }
      }
  }

  def readRDD(targetCellType: CellType)(implicit sc: SparkContext): RDD[(SpatialKey, Raster[MultibandTile])] =
    sc.parallelize(readers, numPartitions)
      .flatMap { reader =>
        val backTransform = Transform(targetCRS, reader.crs)
        val footprint: Polygon = ReprojectRasterExtent.reprojectExtent(reader.rasterExtent, backTransform)
        val keys = mapTransform.keysForGeometry(footprint)

        val rasterExtents = keys.map { key => RasterExtent(mapTransform(key), cols, rows) }
        val rasters = reader.read(rasterExtents, targetCRS, reprojectOptions)

        rasters.map { raster =>
          val center = raster.extent.center
          val k = mapTransform.pointToKey(center)
          (k, raster)
        }
      }

  def readKey(key: SpatialKey): Iterator[T] = ???

  def read(windows: Traversable[RasterExtent]): Iterator[T] = {
    ???
  }

  def asCRS(crs: CRS, cellSize: CellSize): VirtualRaster[T] = ??? // same data in different CRS
}

object VirtualRaster {
  def apply[T](
    readers: Seq[RasterReader2[T]],
    layout: LayoutDefinition,
    targetCRS: CRS,
    reprojectOptions: Reproject.Options
  ): VirtualRaster[T] =
    new VirtualRaster[T](readers, layout, targetCRS, reprojectOptions)

  def apply[T](
    readers: Seq[RasterReader2[T]],
    layout: LayoutDefinition,
    targetCRS: CRS
  ): VirtualRaster[T] =
    new VirtualRaster[T](readers, layout, targetCRS, Reproject.Options.DEFAULT)

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
