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

import geotrellis.layer.Boundable
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster.GridExtent
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent

import jp.ne.opt.chronoscala.Imports._

import java.time.ZonedDateTime

trait Implicits extends Serializable {
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

  implicit val zonedDateTimeBoundable = new Boundable[ZonedDateTime] {
    def minBound(p1: ZonedDateTime, p2: ZonedDateTime): ZonedDateTime = if(p1 <= p2) p1 else p2
    def maxBound(p1: ZonedDateTime, p2: ZonedDateTime): ZonedDateTime = if(p1 > p2) p1 else p2
  }

  implicit val unitBoundable = new Boundable[Unit] {
    def minBound(p1: Unit, p2: Unit): Unit = p1
    def maxBound(p1: Unit, p2: Unit): Unit = p1
  }
}

object Implicits extends Implicits
