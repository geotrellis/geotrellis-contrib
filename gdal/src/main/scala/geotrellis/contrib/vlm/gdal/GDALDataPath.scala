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

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.DataPath

import io.lemonlabs.uri.{UrlWithAuthority, Path}

import java.io.File
import java.net.MalformedURLException


/** Represents and formats a path that points to a files to be read by GDAL.
 *
 *  @param path Path to the file. This path can be formatted in the following
 *    styles: `VSI`, `URI`, or relative path if the file is local. In addition,
 *    this path can be prefixed with, '''gdal+''' to signify that the target GeoTiff
 *    is to be read in only by [[GDALRasterSource]].
 *  @example "/vsizip//vsicurl/http://localhost:8000/files.zip"
 *  @example "s3://bucket/prefix/data.tif"
 *  @example "gdal+file:///tmp/data.tiff"
 *  @note Under normal usage, GDAL requires that all paths to be read be given in its
 *    `VSI Format`. Thus, if given another format type, this class will format it
 *    so that it can be read.
 *
 *  @param compressedFileDelimiter `String` used to represent a file that's to be read
 *    from a compressed.
 *  @example "zip+s3://bucket/prefix/zipped-data.zip!data.tif"
 */
case class GDALDataPath(
  path: String,
  compressedFileDelimiter: String = "!"
) extends DataPath {
  import Schemes._

  val gdalPath =
    UrlWithAuthority.parseOption(path) match {
      case Some(uri) => URIPath(uri)
      case None => RelativePath(Path.parse(path))
    }

  private val badPrefixes: List[String] =
    List("gt+", "gtiff+")

  private val wrongUtility: Boolean =
    badPrefixes
      .map { path.startsWith }
      .reduce { _ || _ }

  require(
    !wrongUtility,
    s"The given path is specified for a different RasterSource type: $path"
  )

  private val pointsToCatalog: Boolean =
    Patterns.QUERY_PARAMS_PATTERN.findFirstIn(path) match {
      case Some(_) => true
      case None => false
    }

  require(
    !pointsToCatalog,
    s"Cannot create a GDALDataPath that points to a GeoTrellis catalog: $path"
  )

  val firstScheme: Option[String] = gdalPath.firstScheme
  val secondScheme: String = gdalPath.secondScheme

  private val isLocal: Boolean =
    secondScheme.contains(FILE) || secondScheme.isEmpty

  // Trying to read something locally on Windows matters
  // because of how file paths on Windows are formatted.
  // Therefore, we need to handle them differently.
  private val onLocalWindows: Boolean =
    System.getProperty("os.name").toLowerCase == "win" && isLocal

  private val formattedPath: String =
    if (gdalPath.targetsNestedFile) {
      val formattedFileName =
        if (onLocalWindows)
          gdalPath.targetFile.replace(compressedFileDelimiter, """\\""")
        else
          gdalPath.targetFile.replace(compressedFileDelimiter, "/")

      val pathWithoutFileName =
        gdalPath.path.substring(0, gdalPath.path.size - formattedFileName.size)

      pathWithoutFileName + formattedFileName
    } else
      gdalPath.path

  private val firstVSIScheme: String =
    firstScheme match {
      case Some(firstHalf) => s"/vsi$firstHalf/"
      case None => ""
    }

  private val secondVSIScheme: String = {
    secondScheme match {
      case (FTP | HTTP | HTTPS) => s"/vsicurl/$formattedPath"
      case S3 => s"/vsis3/$formattedPath"
      case GS => s"/vsigs/$formattedPath"
      case (WASB | WASBS) => s"/vsiaz/$formattedPath"
      case HDFS => s"/vsihdfs/$formattedPath"
      case _ => formattedPath
    }
  }

  /** The given [[path]] in the `VSI` format */
  val vsiPath: String = firstVSIScheme + secondVSIScheme
}

object GDALDataPath {
  def isVSIFormatted(target: String): Boolean =
    Patterns.VSI_PATTERN.findFirstIn(target) match {
      case Some(_) => true
      case None => false
    }

  implicit def toGDALDataPath(path: String): GDALDataPath =
    GDALDataPath(path)
}
