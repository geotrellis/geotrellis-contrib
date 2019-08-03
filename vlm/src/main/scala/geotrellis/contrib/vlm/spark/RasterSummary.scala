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

import geotrellis.contrib.vlm.{LayoutType, RasterSource, ResampleGrid}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, GridExtent}
import geotrellis.layer._
import geotrellis.vector.Extent
import geotrellis.util._

import org.apache.spark.rdd.RDD
import com.typesafe.scalalogging.LazyLogging

case class RasterSummary[K: SpatialComponent: Boundable](
  crs: CRS,
  cellType: CellType,
  cellSize: CellSize,
  extent: Extent,
  cells: Long,
  count: Long,
  // for the internal usage only, required to collect non spatial bounds
  bounds: Bounds[K]
) extends LazyLogging with Serializable {

  def estimatePartitionsNumber: Int = {
    import squants.information._
    val bytes = Bytes(cellType.bytes * cells)
    val numPartitions: Int = math.max((bytes / Megabytes(64)).toInt, 1)
    logger.info(s"Using $numPartitions partitions for ${bytes.toString(Gigabytes)}")
    numPartitions
  }

  def levelFor(layoutScheme: LayoutScheme): LayoutLevel =
    layoutScheme.levelFor(extent, cellSize)

  def toGridExtent: GridExtent[Long] =
    new GridExtent[Long](extent, cellSize)

  def layoutDefinition(scheme: LayoutScheme): LayoutDefinition =
    scheme.levelFor(extent, cellSize).layout

  def combine(other: RasterSummary[K]): RasterSummary[K] = {
    require(other.crs == crs, s"Can't combine LayerExtent for different CRS: $crs, ${other.crs}")
    val smallestCellSize = if (cellSize.resolution < other.cellSize.resolution) cellSize else other.cellSize
    RasterSummary(
      crs,
      cellType.union(other.cellType),
      smallestCellSize,
      extent.combine(other.extent),
      cells + other.cells,
      count + other.count,
      bounds.combine(other.bounds)
    )
  }

  def toTileLayerMetadata(layoutLevel: LayoutLevel): (TileLayerMetadata[K], Some[Int]) = {
    val LayoutLevel(zoom, layoutDefinition) = layoutLevel
    // We want to align the data extent to pixel layout of layoutDefinition
    val layerGridExtent = layoutDefinition.createAlignedGridExtent(extent)
    val keyBounds = bounds match {
      case KeyBounds(minKey, maxKey) =>
        val KeyBounds(minSpatialKey, maxSpatialKey) = KeyBounds(layoutDefinition.mapTransform.extentToBounds(layerGridExtent.extent))
        KeyBounds(minKey.setComponent(minSpatialKey), maxKey.setComponent(maxSpatialKey))
      case EmptyBounds => EmptyBounds
    }
    val tlm = TileLayerMetadata(cellType, layoutDefinition, layerGridExtent.extent, crs, keyBounds)
    (tlm, Some(zoom))
  }

  def toTileLayerMetadata(layoutDefinition: LayoutDefinition, zoom: Int): (TileLayerMetadata[K], Some[Int]) =
    toTileLayerMetadata(LayoutLevel(zoom, layoutDefinition))

  def toTileLayerMetadata(layoutType: LayoutType): (TileLayerMetadata[K], Option[Int]) = {
    val (ld, zoom) = layoutType.layoutDefinitionWithZoom(crs, extent, cellSize)
    val keyBounds = bounds match {
      case KeyBounds(minKey, maxKey) =>
        val KeyBounds(minSpatialKey, maxSpatialKey) = KeyBounds(ld.mapTransform.extentToBounds(extent))
        KeyBounds(minKey.setComponent(minSpatialKey), maxKey.setComponent(maxSpatialKey))
      case EmptyBounds => EmptyBounds
    }
    val tlm = TileLayerMetadata(cellType, ld, extent, crs, keyBounds)
    (tlm, zoom)
  }

  // TODO: probably this function should be removed in the future
  def resample(resampleGrid: ResampleGrid[Long]): RasterSummary[K] = {
    val re = resampleGrid(toGridExtent)
    RasterSummary(
      crs      = crs,
      cellType = cellType,
      cellSize = re.cellSize,
      extent   = re.extent,
      cells    = re.size,
      count    = count,
      bounds   = bounds // do nothing with it, since it contains non spatial information
    )
  }
}

object RasterSummary {
  def collect[K: SpatialComponent: Boundable](rdd: RDD[RasterSource], keyTransform: (RasterSource, SpatialKey) => K): Seq[RasterSummary[K]] = {
    rdd
      .map { rs =>
        val extent = rs.extent
        val crs    = rs.crs
        val key    = keyTransform(rs, SpatialKey(0, 0))
        (crs, RasterSummary(crs, rs.cellType, rs.cellSize, extent, rs.size, 1, KeyBounds(key, key)))
      }
      .reduceByKey { _ combine _ }
      .values
      .collect
      .toSeq
  }

  // TODO: should be refactored, we need to reproject all metadata into a common CRS. This code is for the current code simplification
  def fromRDD[K: SpatialComponent: Boundable](rdd: RDD[RasterSource], keyTransform: (RasterSource, SpatialKey) => K): RasterSummary[K] = {
    /* Notes on why this is awful:
    - scalac can't infer both GetComponent and type V as CellGrid[N]
    - very ad-hoc, why type constraint and lense?
    - might be as simple as HasRasterSummary[V] in the first place
    */
    val all = collect[K](rdd, keyTransform)
    require(all.size == 1, "multiple CRSs detected") // what to do in this case?
    all.head
  }

  def fromRDD(rdd: RDD[RasterSource]): RasterSummary[SpatialKey] = {
    val all = collect(rdd, (_, sk) => sk)
    require(all.size == 1, "multiple CRSs detected")
    all.head
  }

  def fromSeq[K: SpatialComponent: Boundable](seq: Seq[RasterSource], keyTransform: (RasterSource, SpatialKey) => K): RasterSummary[K] = {
    val all =
      seq
        .map { rs =>
          val extent = rs.extent
          val crs    = rs.crs
          val key    = keyTransform(rs, SpatialKey(0, 0))
          (crs, RasterSummary(crs, rs.cellType, rs.cellSize, extent, rs.size, 1, KeyBounds(key, key)))
        }
        .groupBy(_._1)
        .map { case (_, v) => v.map(_._2).reduce(_ combine _) }

    require(all.size == 1, "multiple CRSs detected") // what to do in this case?
    all.head
  }

  def fromSeq(seq: Seq[RasterSource]): RasterSummary[SpatialKey] = fromSeq(seq, (_, sk) => sk)
}
