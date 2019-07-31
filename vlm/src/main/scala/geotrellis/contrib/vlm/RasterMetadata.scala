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

package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, GridExtent, RasterExtent}
import geotrellis.vector.Extent

trait RasterMetadata {
  def crs: CRS
  def bandCount: Int
  def cellType: CellType

  /** Cell size at which rasters will be read when using this [[RasterSource]]
    *
    * Note: some re-sampling of underlying raster data may be required to produce this cell size.
    */
  def cellSize: CellSize = gridExtent.cellSize

  def gridExtent: GridExtent[Long]

  /** All available resolutions for this raster source
    *
    * <li> For base [[RasterSource]] instance this will be resolutions of available overviews.
    * <li> For reprojected [[RasterSource]] these resolutions represent an estimate where
    *      each cell in target CRS has ''approximately'' the same geographic coverage as a cell in the source CRS.
    *
    * When reading raster data the underlying implementation will have to sample from one of these resolutions.
    * It is possible that a read request for a small bounding box will results in significant IO request when the target
    * cell size is much larger than closest available resolution.
    *
    * __Note__: It is expected but not guaranteed that the extent each [[RasterExtent]] in this list will be the same.
    */
  def resolutions: List[GridExtent[Long]]

  def extent: Extent = gridExtent.extent

  /** Raster pixel column count */
  def cols: Long = gridExtent.cols

  /** Raster pixel row count */
  def rows: Long = gridExtent.rows
}

/** Base RasterSourceMetadata used for the RasterSourceMetadata[F] ~> F[RasterSourceMetadata] transformation. */
case class BaseRasterMetadata(
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]]
) extends RasterMetadata
