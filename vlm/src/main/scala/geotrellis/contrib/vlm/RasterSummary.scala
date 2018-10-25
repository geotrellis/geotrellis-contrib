package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster.{CellGrid, CellSize, CellType, RasterExtent}
import geotrellis.spark._
import geotrellis.spark.tiling.{LayoutDefinition, LayoutLevel, LayoutScheme}
import geotrellis.util._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD

case class RasterSummary(
  crs: CRS,
  cellType: CellType,
  cellSize: CellSize,
  extent: Extent,
  cells: Long,
  count: Long
) {
  def levelFor(layoutScheme: LayoutScheme): LayoutLevel =
    layoutScheme.levelFor(extent, cellSize)

  def toRasterExtent: RasterExtent = RasterExtent(extent, cellSize)

  def layoutDefinition(scheme: LayoutScheme): LayoutDefinition = scheme.levelFor(extent, cellSize).layout

  def combine(other: RasterSummary) = {
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

  def toTileLayerMetadata(layoutType: LayoutType) = {
    val (ld, zoom) = layoutType.layoutDefinitionWithZoom(crs, extent, cellSize)
    val dataBounds: Bounds[SpatialKey] = KeyBounds(ld.mapTransform.extentToBounds(extent))
    TileLayerMetadata[SpatialKey](cellType, ld, extent, crs, dataBounds) -> zoom
  }

  def resample(resampleGrid: ResampleGrid): RasterSummary = {
    val re = resampleGrid(toRasterExtent)
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
  def collect[V <: CellGrid: GetComponent[?, ProjectedExtent]](rdd: RDD[V]): Seq[RasterSummary] = {
    rdd
      .map { grid =>
        val ProjectedExtent(extent, crs) = grid.getComponent[ProjectedExtent]
        val cellSize = CellSize(extent, grid.cols, grid.rows)
        (crs, RasterSummary(crs, grid.cellType, cellSize, extent, grid.size, 1))
      }
      .reduceByKey { _ combine _ }
      .values
      .collect
      .toSeq
  }

  // TODO: should be refactored, we need to reproject all metadata into a common CRS
  // this code is for the current code simplification
  def fromRDD[V <: CellGrid: GetComponent[?, ProjectedExtent]](rdd: RDD[V]): RasterSummary = {
    val all = collect[V](rdd)
    require(all.size == 1, "multiple CRSs detected") // what to do in this case?
    all.head
  }
}

