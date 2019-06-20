package geotrellis.contrib.vlm


trait RasterSourceProvider {
  def canProcess(path: String): Boolean

  def rasterSource(path: String): RasterSource
}
