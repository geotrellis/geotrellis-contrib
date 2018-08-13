package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import org.apache.spark._
import org.apache.spark.rdd._
import cats.effect.IO


object TiledRasterRDD {
  def apply(rasters: Seq[TiledRaster], layout: LayoutDefinition): RDD[(SpatialKey, MultibandTile)] = {
    // how do I partition these ?
    // each raster prepresents a region ... which may overlap with N other regions.
    // => I can't start partitioning by raster reader.
    // => I probably shouldn't enumerate the keys because that places a fixed upper bound on how big RDD can be.

    // how do I not produce empty tiles?
    ???
  }
}