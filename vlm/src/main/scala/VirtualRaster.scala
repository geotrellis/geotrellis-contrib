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
import geotrellis.proj4._
import cats.effect.IO


class VirtualRaster[T](readers: RasterReader2[T]) {
    // - we can read multiple files in parallel
    def crs: CRS = ??? // all tiles come out in CRS
    def extent: Extent
    def cols: Int
    def rows: Int

    def read(windows: Traversable[GridBounds]): IO[Option[T]] = {
        // I guess we let each reader perform the CRS conversion.

        ???
    }

    def asCRS(crs: CRS, cellSize: CellSize): VirtualRaster[T] = ??? // same data in different CRS
}

object VirtualRaster {
    case class Source[T](reader: RasterReader2[T], footprint: Polygon)
    object Source {
        def apply[T](reader: RasterReader2[T], target: CRS): Source[T] = ???
    }
}
