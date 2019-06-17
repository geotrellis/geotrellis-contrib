package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.DataPath


case class GeoTrellisDataPath(val path: String) extends DataPath {
  protected def servicePrefix: String = "gt+"

  def targetPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path
}
