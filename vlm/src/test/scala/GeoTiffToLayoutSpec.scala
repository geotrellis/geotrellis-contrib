package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.spark._
import geotrellis.vector._

import org.scalatest._

class GeoTiffToLayoutSpec extends FunSpec {

  val scene: RasterSource = ??? // some LC8 Scene

  // Goal: display both scenes in WebMercator, Zoom 13
  val scheme = ZoomedLayoutScheme(WebMercator)
  val layout: LayoutDefinition = scheme.levelForZoom(13).layout

  val rddLayer: RDD[(SpatialKey, MultibandTile)] with TileLayerMetadata[SpatialKey] =
    RasterSourceRDD(scene, layout)

  // // val rddBufferedLayer: RDD[(SpatialKey, BufferedTile[MultibandTile])] with TileLayerMetadata[SpatialKey] =
  //   RasterSourceRDD.buffered(scene, layout, bufferSize = 5)
}