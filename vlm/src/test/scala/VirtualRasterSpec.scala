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

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
      val layout = scheme.levelForZoom(13).layout

      val virtualRaster = VirtualRaster(List(rasterReader), layout)

      val actual = virtualRaster.readRDD(rasterReader.cellType).count()
      val expected = layout.layoutCols * layout.layoutRows

      actual should be (expected)
    }
  }
}
