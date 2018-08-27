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
  val uri = "file:///tmp/aspect-tiled.tif"
  val rasterSource = new GeoTiffRasterSource(uri)

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
      val layout = scheme.levelForZoom(13).layout

      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(rasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }

      val rdd = RasterSourceRDD(rasterSource, layout)
      val raster = rdd.stitch()
      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }

      for ((actual, expected) <- actualKeys.zip(expectedKeys)) {
        actual should be (expected)
      }

      val actualExtent = actualKeys.map { key => layout.mapTransform(key) }.reduce { _ combine _ }

      layout.extent.covers(actualExtent) should be (true)
    }
  }
}
