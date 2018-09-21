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

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4._
import cats.effect.IO
import java.net.URI

/**
* Single threaded instance of a reader that is able to read windows from larger raster.
* Some initilization step is expected to provide metadata about source raster
*/
trait RasterSource extends Serializable {
    def uri: String
    def extent: Extent
    def crs: CRS
    def cols: Int
    def rows: Int
    def bandCount: Int
    def cellType: CellType
    def rasterExtent = RasterExtent(extent, cols, rows)
    def cellSize = CellSize(extent, cols, rows)
    def gridExtent = GridExtent(extent, cellSize)

    def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): RasterSource

    def reproject(targetCRS: CRS): RasterSource =
        reproject(targetCRS, NearestNeighbor)

    def reproject(targetCRS: CRS, resampleMethod: ResampleMethod): RasterSource =
        withCRS(targetCRS, resampleMethod)

    def reproject(targetCRS: CRS, resampleMethod: ResampleMethod, rasterExtent: RasterExtent): RasterSource

    /** Reads a window for the extent.
      * Return extent may be smaller than requested extent around raster edges.
      * May return None if the requested extent does not overlap the raster extent.
      */
    @throws[IndexOutOfBoundsException]("if requested bands do not exist")
    def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]]

    /** Reads a window for pixel bounds.
      * Return extent may be smaller than requested extent around raster edges.
      * May return None if the requested extent does not overlap the raster extent.
      */
    @throws[IndexOutOfBoundsException]("if requested bands do not exist")
    def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]]

    def read(extent: Extent): Option[Raster[MultibandTile]] =
        read(extent, (0 until bandCount))

    def read(bounds: GridBounds): Option[Raster[MultibandTile]] =
        read(bounds, (0 until bandCount))

    def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
        extents.toIterator.flatMap(read(_, bands).toIterator)

    def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] =
        extents.toIterator.flatMap(read(_, (0 until bandCount)).toIterator)

    def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
        bounds.toIterator.flatMap(read(_, bands).toIterator)

    def readBounds(bounds: Traversable[GridBounds]): Iterator[Raster[MultibandTile]] =
        bounds.toIterator.flatMap(read(_, (0 until bandCount)).toIterator)

}
