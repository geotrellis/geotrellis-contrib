package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import geotrellis.raster.{RasterExtent, CellType, Raster, MultibandTile}


private[vlm] case class RasterViewOptions(
  crs: Option[CRS] = None,
  cellType: Option[CellType] = None,
  rasterExtent: Option[RasterExtent] = None,
  readMethod:  Option[(Extent, Seq[Int]) => Option[Raster[MultibandTile]]] = None
)
