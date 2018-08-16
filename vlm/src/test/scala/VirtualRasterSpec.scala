package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._

import org.apache.spark._

class VirtualRasterSpec extends FunSpec with TestEnvironment {
  val uri = "file:///tmp/landsat-multiband-pixel.tiff"
  val rasterReader = new GeoTiffRasterReader(uri)

  val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
  val layout = scheme.levelForZoom(13).layout

  val virtualRaster = VirtualRaster(List(rasterReader), layout)

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
      val layout = scheme.levelForZoom(13).layout

      val virtualRaster = VirtualRaster(List(rasterReader), layout)

      val actual = virtualRaster.readRDD(rasterReader.cellType).count()
      val expected = layout.layoutCols * layout.layoutRows

      actual should be (expected)
    }

    it("should have the correct keys") {
      val sourceExtent = rasterReader.extent
      val targetExtent =
        Extent(
          sourceExtent.xmin,
          sourceExtent.ymin,
          sourceExtent.xmin + 10000,
          sourceExtent.ymin + 10000
        )

      val targetKeys = layout.mapTransform.keysForGeometry(targetExtent.toPolygon).toSeq

      val actualKeys = virtualRaster.getKeysRDD(targetKeys, rasterReader.cellType).keys.collect()
      val expectedKeys = targetKeys

      val actual = actualKeys.sortBy { key => (key.col, key.row) }
      val expected = expectedKeys.sortBy { key => (key.col, key.row) }

      for ((a, e) <- actual.zip(expected)) {
        a should be (e)
      }

      actual should be (expected)
    }
  }
}
