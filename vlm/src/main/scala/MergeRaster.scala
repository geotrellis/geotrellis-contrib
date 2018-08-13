package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4._
import cats.effect.IO


trait MergeRaster extends RasterSource {
  def rasters: Seq[RasterSource]
  def grid: GridExtent
  // ... our rasters must be pixel aligned (or look like they are?)

  def mergeFunc: List[Raster[MultibandTile]] => Raster[MultibandTile]
  // ... we maybe overlapping merging chips around borders

  def read(window: RasterExtent): Option[Raster[MultibandTile]] = {
    // 1. filter rasters for intersection
    // 2. read chips from each raster
    // 3. merge chips
    None
  }
}