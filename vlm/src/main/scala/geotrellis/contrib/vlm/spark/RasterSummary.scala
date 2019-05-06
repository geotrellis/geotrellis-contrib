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

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.{LayoutType, ResampleGrid, TargetGrid}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellGrid, CellSize, CellType, RasterExtent, GridExtent}
import geotrellis.spark._
import geotrellis.spark.tiling.{LayoutDefinition, LayoutLevel, LayoutScheme}
import geotrellis.util._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import spire.math.Integral
import spire.implicits._

case class RasterSummary(
  crs: CRS,
  cellType: CellType,
  cellSize: CellSize,
  extent: Extent,
  cells: Long,
  count: Long
) extends LazyLogging {

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

  def combine(other: RasterSummary): RasterSummary = {
    require(other.crs == crs, s"Can't combine LayerExtent for different CRS: $crs, ${other.crs}")
    val smallestCellSize = if (cellSize.resolution < other.cellSize.resolution) cellSize else other.cellSize
    RasterSummary(
      crs,
      cellType.union(other.cellType),
      smallestCellSize,
      extent.combine(other.extent),
      cells + other.cells,
      count + other.count
    )
  }

  def toTileLayerMetadata(layoutLevel: LayoutLevel): (TileLayerMetadata[SpatialKey], Some[Int]) = {
    val LayoutLevel(zoom, layoutDefinition) = layoutLevel
    // We want to align the data extent to pixel layout of layoutDefinition
    val layerGridExtent = layoutDefinition.createAlignedGridExtent(extent)
    val dataBounds = KeyBounds(layoutDefinition.mapTransform.extentToBounds(layerGridExtent.extent))
    val tlm = TileLayerMetadata[SpatialKey](cellType, layoutDefinition, layerGridExtent.extent, crs, dataBounds)
    (tlm, Some(zoom))
  }

  def toTileLayerMetadata(layoutDefinition: LayoutDefinition, zoom: Int): (TileLayerMetadata[SpatialKey], Some[Int]) =
    toTileLayerMetadata(LayoutLevel(zoom, layoutDefinition))

  def toTileLayerMetadata(layoutType: LayoutType): (TileLayerMetadata[SpatialKey], Option[Int]) = {
    val (ld, zoom) = layoutType.layoutDefinitionWithZoom(crs, extent, cellSize)
    val dataBounds: Bounds[SpatialKey] = KeyBounds(ld.mapTransform.extentToBounds(extent))
    val tlm = TileLayerMetadata[SpatialKey](cellType, ld, extent, crs, dataBounds)
    (tlm, zoom)
  }

  // TODO: probably this function should be removed in the future
  def resample(resampleGrid: ResampleGrid[Long]): RasterSummary = {
    val re = resampleGrid(toGridExtent)
    RasterSummary(
      crs      = crs,
      cellType = cellType,
      cellSize = re.cellSize,
      extent   = re.extent,
      cells    = re.size,
      count    = count
    )
  }
}

object RasterSummary {
  /** Collect [[RasterSummary]] from unstructred rasters, grouped by CRS */
  def collect[V <: CellGrid[N]: GetComponent[?, ProjectedExtent], N: Integral](rdd: RDD[V]): Seq[RasterSummary] = {
    rdd
      .map { grid =>
        val ProjectedExtent(extent, crs) = grid.getComponent[ProjectedExtent]
        val cellwidth: Double = extent.width / grid.cols.toDouble
        val cellheight: Double = extent.height / grid.rows.toDouble
        val cellSize = CellSize(cellwidth, cellheight)
        (crs, RasterSummary(crs, grid.cellType, cellSize, extent, grid.size.toLong, 1))
      }
      .reduceByKey { _ combine _ }
      .values
      .collect
      .toSeq
  }

  // TODO: should be refactored, we need to reproject all metadata into a common CRS. This code is for the current code simplification
  def fromRDD[V <: CellGrid[N]: GetComponent[?, ProjectedExtent], N: Integral](rdd: RDD[V]): RasterSummary = {
    /* Notes on why this is awful:
    - scalac can't infer both GetComponent and type V as CellGrid[N]
    - very ad-hoc, why type constraint and lense?
    - might be as simple as HasRasterSummary[V] in the first place
    */
    val all = collect[V, N](rdd)
    require(all.size == 1, "multiple CRSs detected") // what to do in this case?
    all.head
  }

  def fromSeq[V <: CellGrid[N]: GetComponent[?, ProjectedExtent], N: Integral](seq: Seq[V]): RasterSummary = {
    val all =
      seq
        .map { grid =>
          val ProjectedExtent(extent, crs) = grid.getComponent[ProjectedExtent]
          val cellwidth: Double = extent.width / grid.cols.toDouble
          val cellheight: Double = extent.height / grid.rows.toDouble
          val cellSize = CellSize(cellwidth, cellheight)
          (crs, RasterSummary(crs, grid.cellType, cellSize, extent, grid.size.toLong, 1))
        }
        .groupBy(_._1)
        .map { case (_, v) => v.map(_._2).reduce(_ combine _) }

    require(all.size == 1, "multiple CRSs detected") // what to do in this case?
    all.head
  }
}
