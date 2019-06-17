package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.DataPath


case class GeoTiffDataPath(val path: String) extends DataPath {
  protected def servicePrefix: String = "gtiff+"

  def targetPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path
}
