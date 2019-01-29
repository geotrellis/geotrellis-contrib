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

import geotrellis.vector.Extent
import geotrellis.raster.{RasterExtent, GridExtent, CellSize}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod

sealed trait ResampleGrid {
  // this is a by name parameter, as we don't need to call the source in all ResampleGrid types
  def apply(source: => GridExtent): GridExtent

  /** This is a temporary adapter
    * @param extent Extent of raster footprint after reprojection.
    */
  def reprojectOptions(extent: => Extent,  method: ResampleMethod): Reproject.Options
}

/** Determines target cell size through pixel column and row dimenions
  * Target `Extent` is determined from the source `Extent`.
  */
case class Dimensions(cols: Int, rows: Int) extends ResampleGrid {
  def apply(source: => GridExtent): GridExtent =
    RasterExtent(source.extent, cols, rows): GridExtent

  def reprojectOptions(extent: => Extent,  method: ResampleMethod): Reproject.Options = {
    val cs = CellSize(width = extent.width / cols,  height = extent.height / rows)
    Reproject.Options(method = method, targetCellSize = Some(cs))
  }
}

/** Align target pixel grid and cell size to given grid.
  * Target `Extent` is determined from the source `Extent`.
  */
case class TargetGrid(grid: GridExtent) extends ResampleGrid {
  def apply(source: => GridExtent): GridExtent =
    grid.createAlignedGridExtent(source.extent)

  def reprojectOptions(extent: => Extent,  method: ResampleMethod): Reproject.Options = {
    Reproject.Options(method = method, parentGridExtent = Some(grid))
  }
}

/** Align target pixel grid, cell size, and Extent to given region.
  * Target `Extent` may be clipped or buffered to given region.
  */
case class TargetRegion(region: RasterExtent) extends ResampleGrid {
  def apply(source: => GridExtent): GridExtent =
    region

  def reprojectOptions(extent: => Extent,  method: ResampleMethod): Reproject.Options = {
    Reproject.Options(method = method, targetRasterExtent = Some(region))
  }
}
