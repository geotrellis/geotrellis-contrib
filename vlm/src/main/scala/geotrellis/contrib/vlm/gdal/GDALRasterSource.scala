package geotrellis.contrib.vlm.gdal

case class GDALRasterSource(uri: String) extends GDALBaseRasterSource {
  val baseWarpList: List[GDALWarpOptions] = Nil
  lazy val warpOptions: GDALWarpOptions = GDALWarpOptions()
}
