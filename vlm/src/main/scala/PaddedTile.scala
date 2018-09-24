package geotrellis.contrib.vlm


import geotrellis.raster.GridBounds


case class PaddedTile(
  actualBounds: GridBounds,
  targetBounds: GridBounds
) {
  def size: Long = targetBounds.size
}
