package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.DataPath


case class GeoTiffDataPath(
  val path: String,
  val tiffExtensions: List[String] = List(".tiff", ".tif")
) extends DataPath {
  private val pointsToTiff: Boolean = {
    val lowerCase = path.toLowerCase

    tiffExtensions.map { lowerCase.endsWith }.reduce { _ || _ }
  }

  require(pointsToTiff, s"The given path must point to a GeoTiff: $path")

  private val badPrefixes: List[String] =
    List(
      "gdal+",
      "zip+",
      "gzip+",
      "gz+",
      "zip+",
      "tar+"
    )

  private val hasBadPrefix: Boolean =
    badPrefixes
      .map { path.startsWith }
      .reduce { _ || _ }

  require(!hasBadPrefix, s"Path points to a GeoTiff that GeoTiffRasterSource cannot read: $path")

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
