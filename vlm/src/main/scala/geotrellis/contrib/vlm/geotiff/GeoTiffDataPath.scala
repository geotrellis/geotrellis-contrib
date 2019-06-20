package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.DataPath


case class GeoTiffDataPath(val path: String) extends DataPath {
  def servicePrefixes: List[String] = List("gtiff+")

  def geoTiffPath: String =
    if (path.startsWith(servicePrefixes.head))
      path.splitAt(servicePrefixes.head.size)._2
    else
      path
}

object GeoTiffDataPath {
  implicit def toGeoTiffDataPath(path: String): GeoTiffDataPath =
    GeoTiffDataPath(path)
}
