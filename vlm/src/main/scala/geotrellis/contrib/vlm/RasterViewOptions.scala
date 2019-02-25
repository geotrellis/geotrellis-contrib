package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import geotrellis.raster.{RasterExtent, CellType, Raster, MultibandTile, GridBounds}


private[vlm] case class RasterViewOptions(
  crs: Option[CRS] = None,
  cellType: Option[CellType] = None,
  rasterExtent: Option[RasterExtent] = None,
  readMethod:  Option[(GridBounds, Seq[Int]) => Option[Raster[MultibandTile]]] = None,
  readExtentMethod:  Option[(Extent, Seq[Int]) => Option[Raster[MultibandTile]]] = None,
  readExtentsMethod:  Option[(Traversable[Extent], Seq[Int]) => Iterator[Raster[MultibandTile]]] = None,
  readBoundsMethod:  Option[(Traversable[GridBounds], Seq[Int]) => Iterator[Raster[MultibandTile]]] = None
)
