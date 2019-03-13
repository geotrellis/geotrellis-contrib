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
import geotrellis.raster._
import geotrellis.vector.Extent

/**
  * Reference to a pixel region in a [[RasterSource]] that may be read at a later time.
  */
trait RasterRegion extends CellGrid with Serializable {
  def raster: Option[Raster[MultibandTile]]
  def extent: Extent
  def crs: CRS
}

object RasterRegion {
  /** Reference to a pixel region in a [[RasterSource]] that may be read at a later time.
    * @note It is required that the [[RasterSource]] intersects with the given [[GridBounds]].
    *
    * @param source raster source that can be used to read this region.
    * @param bounds pixel bounds relative to the source, maybe not be fully contained by the source bounds.
    */
  def apply(source: RasterSource, bounds: GridBounds): RasterRegion =
    GridBoundsRasterRegion(source, bounds)

  case class GridBoundsRasterRegion(source: RasterSource, bounds: GridBounds) extends RasterRegion {
    require(bounds.intersects(source.gridBounds), s"The given bounds: $bounds must intersect the given source: $source")
    @transient lazy val raster: Option[Raster[MultibandTile]] =
      for {
        intersection <- source.gridBounds.intersection(bounds)
        raster <- source.read(intersection)
      } yield {
        if (raster.tile.cols == cols && raster.tile.rows == rows)
          raster
        else {
          val colOffset = math.abs(bounds.colMin - intersection.colMin)
          val rowOffset = math.abs(bounds.rowMin - intersection.rowMin)
          require(colOffset <= Int.MaxValue && rowOffset <= Int.MaxValue, "Computed offsets are outside of RasterBounds")
          raster.mapTile { _.mapBands { (_, band) => PaddedTile(band, colOffset.toInt, rowOffset.toInt, cols, rows) } }
        }
      }

    override def cols: Int = bounds.width
    override def rows: Int = bounds.height
    override def extent: Extent = source.rasterExtent.extentFor(bounds, clamp = false)
    override def crs: CRS = source.crs
    override def cellType: CellType = source.cellType
  }
}
