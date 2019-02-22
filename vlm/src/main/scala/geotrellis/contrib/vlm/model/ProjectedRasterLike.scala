package geotrellis.contrib.vlm.model
import geotrellis.proj4.CRS
import geotrellis.raster.{CellGrid, RasterExtent}
import geotrellis.vector.{Extent, ProjectedExtent}

/**
  * Conformance interface for entities that are tile-like with a projected extent.
  */
trait ProjectedRasterLike extends CellGrid {
  def crs: CRS
  def extent: Extent
  // Can't call this `dimensions` as it's declared in `Grid` with a different return type
  // that can't be mixed in. Would like to fix that in `Grid`. Tuple2 lack interpretability we should be aiming for.
  def dims: TileDimensions = TileDimensions(cols, rows)
  def rasterExtent: RasterExtent = RasterExtent(extent, cols, rows)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)

}
