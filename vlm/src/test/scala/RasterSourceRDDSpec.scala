package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._

import org.apache.spark._

class RasterSourceRDDSpec extends FunSpec with TestEnvironment {
  val uri = "file:///tmp/landsat-multiband-pixel.tiff"
  val rasterSource = new GeoTiffRasterSource(uri)

  val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
  val layout = scheme.levelForZoom(13).layout

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val rdd = RasterSourceRDD.apply(rasterSource, layout)
      val keys = layout.mapTransform.keysForGeometry(rasterSource.extent.toPolygon)

      rdd.count should be (keys.size)
    }
  }
}
