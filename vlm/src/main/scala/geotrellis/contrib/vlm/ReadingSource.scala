package geotrellis.contrib.vlm


case class ReadingSource(
  source: RasterSource,
  sourceBand: Int,
  targetBand: Int
) extends Serializable
