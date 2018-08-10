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


import java.net.URI

/**
* Single threaded instance of a reader that is able to read windows from larger raster.
* Some initilization step is expected to provide metadata about source raster
*/
trait RasterReader2[T] {
    def uri: URI
    def extent: Extent
    def crs: CRS
    def cols: Int
    def rows: Int

    //def read(windows: Traversable[GridBounds]): IO[Option[T]]
    // maybe this will just upgrade an Iterator, maybe it'll actually be parallel

    // read windows in non-native CRS
    def read(windows: Traversable[RasterExtent], crs: CRS): Iterator[Raster[MultibandTile]]

    // TODO: read tiles in underlying layout ?
    // TODO: should we be validating?
}


object RasterReader2 {
  trait Options {
    def partitionBytes: Option[Long]
  }
}
