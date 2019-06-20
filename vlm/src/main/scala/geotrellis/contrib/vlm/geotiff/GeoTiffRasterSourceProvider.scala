package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._


class GeoTiffRasterSourceProvider extends RasterSourceProvider {
  def canProcess(path: String): Boolean =
    try {
      GeoTiffDataPath(path)
      return true
    } catch {
      case _: Throwable => false
    }

  def rasterSource(path: String): RasterSource =
    GeoTiffRasterSource(path)
}
