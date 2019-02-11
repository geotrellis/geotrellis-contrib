package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster.{RasterExtent, CellType}


private[vlm] case class RasterViewOptions(
  crs: Option[CRS] = None,
  cellType: Option[CellType] = None,
  rasterExtent: Option[RasterExtent] = None
)
