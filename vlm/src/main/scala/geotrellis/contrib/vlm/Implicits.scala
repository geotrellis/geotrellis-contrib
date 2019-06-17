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

import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster.GridExtent
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent

trait Implicits extends Serializable {
  implicit def toString(dataPath: DataPath): String = dataPath.toString

  implicit class gridExtentMethods[N: spire.math.Integral](self: GridExtent[N]) {
    def reproject(src: CRS, dest: CRS, options: Options): GridExtent[N] =
      if(src == dest) self
      else {
        val transform = Transform(src, dest)
        options
          .targetRasterExtent
          .map(_.toGridType[N])
          .getOrElse(ReprojectRasterExtent(self, transform, options = options))
      }

    def reproject(src: CRS, dest: CRS): GridExtent[N] = reproject(src, dest, Options.DEFAULT)
  }
}

object Implicits extends Implicits
