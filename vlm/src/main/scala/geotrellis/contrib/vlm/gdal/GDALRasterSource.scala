package geotrellis.contrib.vlm.gdal

case class GDALRasterSource(uri: String) extends GDALBaseRasterSource {
  @transient lazy val dataset = baseDataset
}
