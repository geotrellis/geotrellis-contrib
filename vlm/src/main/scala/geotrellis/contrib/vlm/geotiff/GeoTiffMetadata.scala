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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.SourceMetadata
import geotrellis.raster.io.geotiff.Tags

case class GeoTiffMetadata(tags: Tags) extends SourceMetadata {
  def base: Map[String, String] = tags.headTags
  def band(b: Int): Map[String, String] = if(b == 0) base else tags.bandTags.lift(b).getOrElse(Map())
  def combine(self: SourceMetadata, bandCount: Int): GeoTiffMetadata =
    GeoTiffMetadata(Tags(base, (1 until bandCount).map(band).toList))
}
