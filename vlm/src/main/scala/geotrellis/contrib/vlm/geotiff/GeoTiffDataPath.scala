package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.DataPath


case class GeoTiffDataPath(val path: String) extends DataPath {
  private val servicePrefix: String = "gtiff+"

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
