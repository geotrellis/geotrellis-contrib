package geotrellis.contrib.vlm


case class ReadingSource(
  source: RasterSource,
  sourceToTargetBand: Map[Int, Int]
) extends Serializable

object ReadingSource {
  def apply(source: RasterSource, sourceBand: Int, targetBand: Int): ReadingSource =
    ReadingSource(source, Map(sourceBand -> targetBand))

  def apply(source: RasterSource, targetBand: Int): ReadingSource =
    apply(source, 0, targetBand)
}
