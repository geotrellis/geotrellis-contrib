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
 *  @param tiffExtensions `List` of extensions to look for when confirming whether or
 *    not the target file is a GeoTiff.
 *  @note Capitalization of the extension is not regarded.
 */
case class GeoTiffDataPath(
  val path: String,
  val tiffExtensions: List[String] = List(".tiff", ".tif")
) extends DataPath {
  private val pointsToTiff: Boolean = {
    val lowerCase = path.toLowerCase

    tiffExtensions.map { lowerCase.endsWith }.reduce { _ || _ }
  }

  require(pointsToTiff, s"The given path must point to a GeoTiff: $path")

  private val badPrefixes: List[String] =
    List(
      "gdal+",
      "zip+",
      "gzip+",
      "gz+",
      "zip+",
      "tar+"
    )

  private val hasBadPrefix: Boolean =
    badPrefixes
      .map { path.startsWith }
      .reduce { _ || _ }

  require(!hasBadPrefix, s"Path points to a GeoTiff that GeoTiffRasterSource cannot read: $path")

  private val servicePrefix: String = "gtiff+"

  /** The formatted path to the GeoTiff that will be read */
  def geoTiffPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path
}

object GeoTiffDataPath {
  implicit def toGeoTiffDataPath(path: String): GeoTiffDataPath =
    GeoTiffDataPath(path)
}
