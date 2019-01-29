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

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.raster._

/** Reference to a pixel region in a [[RasterSource]] that may be read at a later time.
  * @note It is required that the [[RasterSource]] intersects with the given [[GridBounds]].
  *
  * @param source raster source that can be used to read this region.
  * @param extent Extent extent of region pixels, should be aligned to source pixl grid
  */
case class RasterRegion(
  source: RasterSource,
  extent: Extent
) extends CellGrid with Serializable {
  @transient lazy val raster: Option[Raster[MultibandTile]] =
    for {
      raster <- source.read(rasterExtent.extent)
    } yield {
      if (raster.tile.cols == rasterExtent.cols && raster.tile.rows == rasterExtent.rows)
        raster
      else { // we have a raster thats smaller than desired
        val gb = rasterExtent.gridBoundsFor(raster.extent)
        val tile = raster.tile.mapBands { (_, band) =>
          PaddedTile(band, gb.colMin, gb.rowMin, cols, rows)
      }
        Raster(tile, rasterExtent.extent)
      }
    }

  def cols: Int = rasterExtent.cols
  def rows: Int = rasterExtent.rows
  def crs: CRS = source.crs
  def cellType: CellType = source.cellType
  def rasterExtent: RasterExtent = source.gridExtent.createAlignedRasterExtent(extent)
  def projectedExtent: ProjectedExtent = ProjectedExtent(rasterExtent.extent, crs)
}
