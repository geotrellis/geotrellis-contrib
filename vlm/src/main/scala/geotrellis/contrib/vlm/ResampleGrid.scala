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

import geotrellis.raster.{RasterExtent, GridExtent}

sealed trait ResampleGrid {
  // this is a by name parameter, as we don't need to call the source in all ResampleGrid types
  def apply(source: => RasterExtent): RasterExtent
}
object ResampleGrid {

  case class Dimensions(cols: Int, rows: Int) extends ResampleGrid {
    def apply(source: => RasterExtent): RasterExtent =
      RasterExtent(source.extent, cols, rows)
  }

  case class TargetGrid(grid: GridExtent) extends ResampleGrid {
    def apply(source: => RasterExtent): RasterExtent =
      grid.createAlignedRasterExtent(source.extent)
  }

  case class TargetRegion(region: RasterExtent) extends ResampleGrid {
    def apply(source: => RasterExtent): RasterExtent =
      region
  }
}
