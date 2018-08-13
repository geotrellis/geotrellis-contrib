package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent}
import geotrellis.vector._

trait WarpRasterReader extends RasterSource {
  def base: RasterSource
  def crs: CRS
  def rasterExtent: RasterExtent

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val transform = Transform(base.crs, crs)
    val backTransform = Transform(crs, base.crs)

    val intersectingWindows: Map[GridBounds, RasterExtent] =
      windows.flatMap { case targetRasterExtent =>
        val sourceExtent: Extent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
        if (sourceExtent.intersects(base.extent)) {
          val sourcePixelBounds: GridBounds = base.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
          Some((sourcePixelBounds, targetRasterExtent))
        } else None
        // should I even filter here ?
      }.toMap

    ???
    // tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
    //   val targetRasterExtent = intersectingWindows(gb)
    //   val sourceRaster = Raster(tile.convert(targetCellType), tiff.rasterExtent.extentFor(gb))
    //   // !!! Here we need reproject type class on T
    //   sourceRaster.reproject(targetRasterExtent, transform, backTransform, options)

    // }
    // NEED: HasCellType[T],
    // NEED: Raster[T](tile: T, extent: Extent), HasCells[T]
    // this is VERY quickly spinning out of control because of T
  }
}