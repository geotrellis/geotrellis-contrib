/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.model
import geotrellis.raster.{CellSize, RasterExtent}
import geotrellis.vector.Extent

import scala.Integral.Implicits._

sealed abstract class GriddedExtent[@specialized(Int, Long) N: Integral] extends Grid[N] {
  def extent: Extent
  def cellSize: CellSize = CellSize(extent.width / cols.toDouble(), extent.width / rows.toDouble())

  /**
   * Gets the Extent that matches the grid bounds passed in, aligned
   * with this RasterExtent.
   *
   * The 'clamp' parameter determines whether or not to clamp the
   * Extent to the extent of this RasterExtent; defaults to true. If
   * true, the returned extent will be contained by this
   * RasterExtent's extent, if false, the Extent returned can be
   * outside of this RasterExtent's extent.
   *
   * @param  gridBounds  The extent to get the grid bounds for
   * @param  clamp       A boolean which controlls the clamping behvior
   */
  def extentFor[M: Integral](gridBounds: GridBounds[M], clamp: Boolean = true): Extent = {
    import math._
    import Integral.Implicits._
    val cs = cellSize

    val xmin = gridBounds.colMin.toDouble * cs.width + extent.xmin
    val ymax = extent.ymax - (gridBounds.rowMin.toDouble * cs.height)
    val xmax = xmin + (gridBounds.width.toDouble * cs.width)
    val ymin = ymax - (gridBounds.height.toDouble * cs.height)

    if (clamp) {
      Extent(
        max(min(xmin, extent.xmax), extent.xmin),
        max(min(ymin, extent.ymax), extent.ymin),
        max(min(xmax, extent.xmax), extent.xmin),
        max(min(ymax, extent.ymax), extent.ymin)
      )
    } else {
      Extent(xmin, ymin, xmax, ymax)
    }
  }


  /**
    * Gets the GridBounds aligned with this RasterExtent that is the
    * smallest subgrid of containing all points within the extent. The
    * extent is considered inclusive on it's north and west borders,
    * exclusive on it's east and south borders.  See [[RasterExtent]]
    * for a discussion of grid and extent boundary concepts.
    *
    * The 'clamp' flag determines whether or not to clamp the
    * GridBounds to the RasterExtent; defaults to true. If false,
    * GridBounds can contain negative values, or values outside of
    * this RasterExtent's boundaries.
    *
    * @param     subExtent      The extent to get the grid bounds for
    * @param     clamp          A boolean
    */
  def gridBoundsFor[M >: N: GridBounds.Ctor](subExtent: Extent, clamp: Boolean = true): GridBounds[N] = {

    val longGB = {
      def mapToGrid(mapCoord: (Double, Double)): (Long, Long) = {
        val (x, y) = mapCoord
        mapToGrid(x, y)
      }
      def mapXToGridDouble(x: Double): Double = (x - extent.xmin) / cellSize.width
      def mapYToGridDouble(y: Double): Double = (extent.ymax - y) / cellSize.height

      // West and North boundaries are a simple mapToGrid call.
      val (colMin, rowMin) = mapToGrid(subExtent.xmin, subExtent.ymax)

      // If South East corner is on grid border lines, we want to still only include
      // what is to the West and\or North of the point. However if the border point
      // is not directly on a grid division, include the whole row and/or column that
      // contains the point.
      val colMax = {
        val colMaxDouble = mapXToGridDouble(subExtent.xmax)
        if (math.abs(colMaxDouble - math.floor(colMaxDouble)) < RasterExtent.epsilon)
          colMaxDouble.toLong - 1L
        else colMaxDouble.toLong
      }

      val rowMax = {
        val rowMaxDouble = mapYToGridDouble(subExtent.ymin)
        if (math.abs(rowMaxDouble - math.floor(rowMaxDouble)) < RasterExtent.epsilon)
          rowMaxDouble.toLong - 1
        else rowMaxDouble.toLong
      }

      GridBounds(colMin, rowMin, colMax, rowMax)
    }

    val clamped = if(clamp)  clampBounds(longGB) else longGB

    clamped.shrink[N].getOrElse(
      throw OverflowException("Unable to extract grid bounds into underlying data size.")
    )
  }

  private def clampBounds(gb: GridBounds[Long]): GridBounds[Long] = {
    import math._
    GridBounds(
      min(max(gb.colMin, 0L), (cols - `1`).toLong),
      min(max(gb.rowMin, 0L), (rows - `1`).toLong),
      min(max(gb.colMax, 0L), (cols - `1`).toLong),
      min(max(gb.rowMax, 0L), (rows - `1`).toLong)
    )
  }
}

object GriddedExtent {

  def apply(extent: Extent, cols: Int, rows: Int): GriddedExtent[Int] = RasterGridExtent(extent, Dimensions(cols, rows))
  def apply(extent: Extent, cols: Long, rows: Long) = LayerGridExtent(extent, Dimensions(cols, rows))
  def apply(extent: Extent, dims: Dimensions[Int])(implicit d: DummyImplicit): GriddedExtent[Int] = RasterGridExtent(extent, dims)
  def apply(extent: Extent, dims: Dimensions[Long]): GriddedExtent[Long] = LayerGridExtent(extent, dims)

  case class RasterGridExtent(extent: Extent, dimensions: Dimensions[Int]) extends GriddedExtent[Int] {
    override def gridBounds: GridBounds[Int] = GridBounds(dimensions)
  }
  case class LayerGridExtent(extent: Extent, dimensions: Dimensions[Long])
  extends GriddedExtent[Long] {
    override def gridBounds: GridBounds[Long] = GridBounds(dimensions)
  }
}
