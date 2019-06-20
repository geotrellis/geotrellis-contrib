package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.DataPath


case class GeoTrellisDataPath(val path: String) extends DataPath {
  def servicePrefixes: List[String] = List("gt+")

  private val strippedPath: String =
    if (path.startsWith(servicePrefixes.head))
      path.splitAt(servicePrefixes.head.size)._2
    else
      path

  val Array(catalogPath, queryParameters) = strippedPath.split('?')

  private val splitQueryParameters: List[String] = queryParameters.split('&').toList

  val layerName: String = splitQueryParameters.head

  val zoomLevel: Option[Int] =
    splitQueryParameters.tail match {
      case List() => None
      case zoom :: _ => Some(zoom.toInt)
    }
}

object GeoTrellisDataPath {
  implicit def toGeoTrellisDataPath(path: String): GeoTrellisDataPath =
    GeoTrellisDataPath(path)
}
