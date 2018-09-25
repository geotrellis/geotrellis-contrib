package geotrellis.contrib.vlm


import geotrellis.raster.GridBounds
import geotrellis.vector.Extent


case class PaddedTile(
  actualBounds: GridBounds,
  targetBounds: GridBounds,
  sourceExtent: Extent,
  colOffset: Int,
  rowOffset: Int
) {
  def size: Long = targetBounds.size
}
