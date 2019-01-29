package geotrellis.contrib.vlm


case class ReadingSource(
  source: RasterSource,
  sourceToTargetBand: Map[Int, Int]
) extends Serializable
