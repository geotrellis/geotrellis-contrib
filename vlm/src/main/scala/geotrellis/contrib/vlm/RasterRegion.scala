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
  * @param bounds pixel bounds relative to the source, maybe not be fully contained by the source bounds.
  */
case class RasterRegion(
  source: RasterSource,
  bounds: GridBounds
) extends CellGrid with Serializable {
  require(bounds.intersects(source.gridBounds), s"The given bounds: $bounds must intersect the given source: $source")

  @transient lazy val raster: Option[Raster[MultibandTile]] =
    source.read(bounds)

  def cols: Int = bounds.width
  def rows: Int = bounds.height
  def extent: Extent = source.rasterExtent.extentFor(bounds, clamp = false)
  def crs: CRS = source.crs
  def cellType: CellType = source.cellType
  def rasterExtent: RasterExtent = RasterExtent(extent, cols, rows)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
}
