package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.DataPath


case class GeoTrellisDataPath(val path: String) extends DataPath {
  private val servicePrefix: String = "gt+"

  private val layerNameParam: String = "layer="
  private val zoomLevelParam: String = "zoom="

  require(path.contains(layerNameParam), s"The layer query parameter must be in the given path: $path")

  // TODO: Support having the zoom parameter be optional
  require(path.contains(zoomLevelParam), s"The zoom query parameter must be in the given path: $path")

  private val strippedPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path

  val Array(catalogPath, queryParameters) = strippedPath.split('?')

  private val splitQueryParameters: List[String] = queryParameters.split('&').toList

  val layerName: String =
    splitQueryParameters
      .head
      .splitAt(layerNameParam.size)
      ._2

  val zoomLevel: Option[Int] =
    splitQueryParameters.tail match {
      case List() => None
      case zoom :: _ =>
        Some(zoom.splitAt(zoomLevelParam.size)._2.toInt)
    }
}

object GeoTrellisDataPath {
  implicit def toGeoTrellisDataPath(path: String): GeoTrellisDataPath =
    GeoTrellisDataPath(path)
}
