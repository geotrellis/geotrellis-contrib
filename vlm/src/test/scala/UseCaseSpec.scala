package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._

import org.apache.spark._

class UseCaseSpec extends FunSpec with TestEnvironment {
  /*
  val landsat1: RasterReader = ??? // LC8 scene in UTM 15
  val landsat2: RasterReader = ??? // LC8 scene in UTM 14

  // Goal: display both scenes in WebMercator, Zoom 13
  val scheme = ZoomedLayoutScheme(WebMercator)
  val layout = scheme.levelForZoom(13).layout
  val gridExtent = layout: GridExtent

  // When we reproject each raster must stay pixel aligned
  val wmScenes = List(
    landsat1.warpToGrid(WebMercator, gridExtent),
    landsat2.warpToGrid(WebMercator, gridExtent))

  val mosaic = MergeRaster(wmScenes, gridExtent)
  // ... its odd that we give gridExtent again

  val tiledRaster = TiledRaster(mosaic, layout)
  tiledRaster.read(SpatialKey(1, 1))
  // ... how does TiledRaster know which part of mosaic are blank ?
  // ??? do we accept sparse rasters? We must. If VLM can be source of global mosaic, it must be sparse.

  // is TiledRasterRDD a duplication of MergeRaster? It almost must be?
  // - Unless we propagate footprints we need to have individual "reasonalby dense" rasters

  val rdd: RDD[(SpatialKey, MultibandTile)] =
    TiledRasterRDD(tiledRaster, partitioner)

  // TODO:
  // 1. Consider how we avoid re-re-resample stack
  */
}
