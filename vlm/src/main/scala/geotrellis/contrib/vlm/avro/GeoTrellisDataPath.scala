package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.DataPath


case class GeoTrellisDataPath(val path: String) extends DataPath {
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

  val layerName =
    brokenUpParams
      .filter { case (k, _) => k == layerNameParam }
      .head
      ._2

  private val mappedQueryParams: Map[String, Int] =
    (brokenUpParams.toMap - layerNameParam).mapValues { _.toInt }

  val zoomLevel: Option[Int] = mappedQueryParams.get(zoomLevelParam)
  val bandCount: Option[Int] = mappedQueryParams.get(bandCountParam)
}

object GeoTrellisDataPath {
  implicit def toGeoTrellisDataPath(path: String): GeoTrellisDataPath =
    GeoTrellisDataPath(path)
}
