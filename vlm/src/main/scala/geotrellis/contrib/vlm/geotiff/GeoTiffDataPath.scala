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

import geotrellis.contrib.vlm.DataPath

import io.lemonlabs.uri.Uri
import io.lemonlabs.uri.encoding.PercentEncoder
import io.lemonlabs.uri.encoding.PercentEncoder.PATH_CHARS_TO_ENCODE

/** Represents a path that points to a GeoTiff to be read.
 *  @note The target file must have a file extension.
 *
 *  @param path Path to a GeoTiff. There are two ways to format this `String`: either
 *    in the `URI` format, or as a relative path if the file is local. In addition,
 *    this path can be prefixed with, '''gtiff+''' to signify that the target GeoTiff
 *    is to be read in only by [[GeoTiffRasterSource]].
 *  @example "data/my-data.tiff"
 *  @example "s3://bucket/prefix/data.tif"
 *  @example "gtiff+file:///tmp/data.tiff"
 *
 *  @note Capitalization of the extension is not regarded.
 */
case class GeoTiffDataPath(path: String, percentEncoder: PercentEncoder = PercentEncoder(PATH_CHARS_TO_ENCODE ++ Set('%', '?', '#'))) extends DataPath {
  private val upath = percentEncoder.encode(path, "UTF-8")

  /** The formatted path to the GeoTiff that will be read */
  lazy val geoTiffPath: String = {
    Uri.parseOption(upath).fold("") { uri =>
      uri.schemeOption.fold(uri.toStringRaw) { scheme =>
        uri.withScheme(scheme.split("\\+").last).toStringRaw
      }
    }
  }
}

object GeoTiffDataPath {
  val PREFIX = "gtiff+"

  implicit def toGeoTiffDataPath(path: String): GeoTiffDataPath = GeoTiffDataPath(path)
}
