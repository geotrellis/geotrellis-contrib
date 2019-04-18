/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.testkit
import geotrellis.contrib.vlm.Resource

trait TestRasterData {
  val allTifExamples: Seq[String] = Seq(
    "img/all-ones.tif",
    "img/aspect-tiled-0-1-2.tif",
    "img/aspect-tiled-0.tif",
    "img/aspect-tiled-1.tif",
    "img/aspect-tiled-2.tif",
    "img/aspect-tiled-3.tif",
    "img/aspect-tiled-4.tif",
    "img/aspect-tiled-5.tif",
    "img/aspect-tiled-bilinear-linux.tif",
    "img/aspect-tiled-bilinear.tif",
    "img/aspect-tiled-near-merc-rdd.tif",
    "img/aspect-tiled-near.tif",
    "img/aspect-tiled.tif",
    "img/badnodata.tif",
    "img/diagonal.tif",
    "img/extent-bug.tif",
    "img/geotiff-at-origin.tif",
    "img/geotiff-off-origin.tif",
    "img/gradient.tif",
    "img/left-to-right.tif",
    "img/multiband.tif",
    "img/slope.tif",
    "img/top-to-bottom.tif"
  )
  def allTifPaths: Seq[String] = allTifExamples.map(Resource.path)
}
object TestRasterData extends TestRasterData
