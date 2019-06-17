package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.DataPath

import java.net.MalformedURLException


case class GDALDataPath(val path: String) extends DataPath {
  protected def servicePrefix: String = "gdal+"

  private lazy val strippedPath: String =
    if (path.startsWith(servicePrefix))
      path.splitAt(servicePrefix.size)._2
    else
      path

  private lazy val vsiPath: VSIPath = VSIPath(strippedPath)

  def targetPath: String =
    try {
      vsiPath.vsiPath
    } catch {
      case _: Throwable =>
        throw new MalformedURLException(
          s"Invalid URI passed into the GDALRasterSource constructor: ${path}." +
          s"Check geotrellis.contrib.vlm.gdal.VSIPath constrains, " +
          s"or pass VSI formatted String into the GDALRasterSource constructor manually."
        )
    }
}
