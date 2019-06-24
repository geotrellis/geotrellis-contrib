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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.DataPath


/** Represents a path that points to a GeoTrellis layer saved in a catalog.
 *
 *  @param path Path to the layer. This can be either an Avro or COG layer.
 *    The given path needs to be in a `URI` format that include the following query
 *    parameters:
 *      - '''layer''': The name of the layer.
 *      - '''zoom''': The zoom level to be read.
 *      - '''band_count''': The number of bands of each Tile in the layer.
 *    Of the above three parameters, `layer` and `zoom` are required. In addition,
 *    this path can be prefixed with, '''gt+''' to signify that the target path
 *    is to be read in only by [[GeotrellisRasterSource]].
 *  @example "s3://bucket/catalog?layer=layer_name&zoom=10"
 *  @example "hdfs://data-folder/catalog?layer=name&zoom-12&band_count=5"
 *  @example "gt+file:///tmp/catalog?layer=name&zoom=5"
 *  @note The order of the query parameters does not matter.
 */
case class GeoTrellisDataPath(path: String) extends DataPath {
  private val servicePrefix: String = "gt+"

  private val layerNameParam: String = "layer"
  private val zoomLevelParam: String = "zoom"
  private val bandCountParam: String = "band_count"

  require(path.contains(s"$layerNameParam="), s"The layer query parameter must be in the given path: $path")

  // TODO: Support having the zoom parameter be optional
  require(path.contains(s"$zoomLevelParam="), s"The zoom query parameter must be in the given path: $path")

  private val strippedPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path

  val Array(catalogPath, queryParams) = strippedPath.split('?')

  private val brokenUpParams: Array[(String, String)] =
    queryParams
      .split('&')
      .map {
        _.split('=') match {
          case Array(k, v) => (k, v)
        }
      }

  /** The name of the target layer */
  val layerName =
    brokenUpParams
      .filter { case (k, _) => k == layerNameParam }
      .head
      ._2

  private val mappedQueryParams: Map[String, Int] =
    (brokenUpParams.toMap - layerNameParam).mapValues { _.toInt }

  /** The zoom level of the target layer */
  val zoomLevel: Option[Int] = mappedQueryParams.get(zoomLevelParam)

  /** The band count of the target layer */
  val bandCount: Option[Int] = mappedQueryParams.get(bandCountParam)
}

object GeoTrellisDataPath {
  implicit def toGeoTrellisDataPath(path: String): GeoTrellisDataPath =
    GeoTrellisDataPath(path)
}
