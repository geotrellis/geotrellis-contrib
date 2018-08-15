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
  val uri = "file:///tmp/landsat-tiled-multiband.tiff"
  val rasterSource = new GeoTiffRasterSource(uri)

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val scheme = ZoomedLayoutScheme(CRS.fromEpsgCode(3857))
      val layout = scheme.levelForZoom(13).layout

      val keys = layout.mapTransform.keysForGeometry(rasterSource.extent.toPolygon)

      val rdd = RasterSourceRDD(rasterSource, layout)

      rdd.count should be (keys.size)
    }

    it("should have the right number of tiles when reprojecting") {
      val crs = CRS.fromEpsgCode(4326)

      val scheme = ZoomedLayoutScheme(crs)
      val layout = scheme.levelForZoom(13).layout

      val reprojectedExtent =
        ProjectedExtent(rasterSource.extent, rasterSource.crs)
          .reprojectAsPolygon(crs)
          .envelope

      val keys = layout.mapTransform.keysForGeometry(reprojectedExtent.toPolygon)

      val warpRasterSource = WarpRasterSource(rasterSource, crs)
      val rdd = RasterSourceRDD(warpRasterSource, layout)

      rdd.count should be (keys.size)
    }
  }
}
