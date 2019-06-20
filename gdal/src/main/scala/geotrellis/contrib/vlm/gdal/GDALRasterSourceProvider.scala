package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.RasterSourceProvider


class GDALRasterSourceProvider extends RasterSourceProvider {
  def canProcess(path: String): Boolean =
    try {
      GDALDataPath(path)
      return true
    } catch {
      case _: Throwable => false
    }

  def rasterSource(path: String): GDALRasterSource =
    GDALRasterSource(path)
}
