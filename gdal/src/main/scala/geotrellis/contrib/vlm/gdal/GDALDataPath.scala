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

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.DataPath

import io.lemonlabs.uri._


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

  val isVSIFormatted: Boolean =
    path.startsWith("/vsi")

  // scala-uri has problems encoding/decoding certain characters,
  // so to account for that, we check to see if the path can
  // be parsed at all. If it can't then we use the UrlPath
  // to encode the path so that it can be used by the
  // classes in the library
  private val gdalPath =
  if (isVSIFormatted)
    VSIPath(path)
  else
    Url.parseOption(path) match {
      case Some(uri) => URIPath(uri)
      case None =>
        val encodedPath = UrlPath.fromRaw(path).toString
        URIPath(Url.parse(encodedPath))
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

  // If the path contains a compressed file delimiter, then we
  // must convert that character into its respective path character.
  // Otherwise, the path can be used as is.
  private val formattedPath: String =
  if (gdalPath.targetsNestedFile && !isVSIFormatted) {
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
      case Some(firstHalf) =>
        if (isVSIFormatted)
          firstHalf
        else
          s"/vsi$firstHalf/"
      case None => ""
    }

  private val secondVSIScheme: String =
    if (!isVSIFormatted)
      secondScheme match {
        case (FTP | HTTP | HTTPS) => "/vsicurl/"
        case S3 => "/vsis3/"
        case GS => "/vsigs/"
        case (WASB | WASBS) => "/vsiaz/"
        case HDFS => s"/vsihdfs/"
        case _ => ""
      }
    else
      secondScheme

  /** The given [[path]] in the `VSI` format */
  val vsiPath: String = firstVSIScheme + secondVSIScheme + formattedPath
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
