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
import geotrellis.contrib.vlm.model.Dimensions.LayerDimensions
import geotrellis.contrib.vlm.model.GridBounds.RasterGridBounds
import geotrellis.raster.{GridBounds => _, _}
import geotrellis.vector.Extent

import scala.math.{max, min}

///** Similar to a RasterExtent, except that it uses Longs for dimensions. */
//case class LayerGridExtent(extent: Extent, grid: Grid.LayerGrid) {
//
//  /**
//    * Gets the GridBounds aligned with this RasterExtent that is the
//    * smallest subgrid of containing all points within the extent. The
//    * extent is considered inclusive on it's north and west borders,
//    * exclusive on it's east and south borders.  See [[RasterExtent]]
//    * for a discussion of grid and extent boundary concepts.
//    *
//    * The 'clamp' flag determines whether or not to clamp the
//    * GridBounds to the RasterExtent; defaults to true. If false,
//    * GridBounds can contain negative values, or values outside of
//    * this RasterExtent's boundaries.
//    *
//    * @param     subExtent      The extent to get the grid bounds for
//    * @param     clamp          A boolean
//    */
//  @deprecated("This is a legacy grid model adapter", "now")
//  def gridBoundsFor(subExtent: Extent, clamp: Boolean = true) = {
//    toLegacyRasterExtent.map(_.gridBoundsFor(subExtent, clamp)).getOrElse(
//      throw new IllegalArgumentException("LayerGrid is too large to convert to GridBounds ")
//    )
//  }
//
//  /**
//    * Gets the Extent that matches the grid bounds passed in, aligned
//    * with this RasterExtent.
//    *
//    * The 'clamp' parameter determines whether or not to clamp the
//    * Extent to the extent of this RasterExtent; defaults to true. If
//    * true, the returned extent will be contained by this
//    * RasterExtent's extent, if false, the Extent returned can be
//    * outside of this RasterExtent's extent.
//    *
//    * @param  gridBounds  The extent to get the grid bounds for
//    * @param  clamp       A boolean which controlls the clamping behvior
//    */
//  def extentFor[N: Integral](gridBounds: GridBounds[N], clamp: Boolean = true): Extent = {
//    import Integral.Implicits._
//    val cs = cellSize
//
//    val xmin = gridBounds.colMin.toDouble * cs.width + extent.xmin
//    val ymax = extent.ymax - (gridBounds.rowMin.toDouble * cs.height)
//    val xmax = xmin + (gridBounds.width.toDouble * cs.width)
//    val ymin = ymax - (gridBounds.height.toDouble * cs.height)
//
//    if(clamp) {
//      Extent(
//        max(min(xmin, extent.xmax), extent.xmin),
//        max(min(ymin, extent.ymax), extent.ymin),
//        max(min(xmax, extent.xmax), extent.xmin),
//        max(min(ymax, extent.ymax), extent.ymin)
//      )
//    } else {
//      Extent(xmin, ymin, xmax, ymax)
//    }
//  }
//
//  def cellSize: CellSize = CellSize(extent.width / cols, extent.width / rows)
//  def cols: Long = grid.cols
//  def rows: Long = grid.rows
//  // This is safe because GridExtent works relative to cellsize, not the integer grid dimensions.
//  def toGridExtent: GridExtent = GridExtent(extent, cellSize)
//  def toLegacyRasterExtent: Option[RasterExtent] = {
//    val s = implicitly[Shrinking[Long, Int]]
//    val ss = cellSize
//    for {
//      c <- s(cols)
//      r <- s(rows)
//    } yield  new RasterExtent(extent,  ss.width, ss.height, c, r)
//  }
//}
//object LayerGridExtent {
//  def apply(rasterExtent: RasterExtent): LayerGridExtent =
//    LayerGridExtent(rasterExtent.extent, Grid(rasterExtent.cols, rasterExtent.rows))
//
//  def apply(extent: Extent, cols: Long, rows: Long): LayerGridExtent =
//    LayerGridExtent(extent, Grid(cols, rows))
//}
