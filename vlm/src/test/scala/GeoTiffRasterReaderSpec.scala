package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.spark._
import geotrellis.vector._

import org.scalatest._

class GeoTiffRasterReaderSpec extends FunSpec {
  /*
    val uri = "file:/Users/eugene/Downloads/LC08_L1TP_128034_20170814_20170825_01_T1_B3.TIF"
    val reader = new GeoTiffRasterReader(uri)

    it("returns empty iterator for non-intersecting extent") {
        val it = reader.read(List(RasterExtent(Extent(0,0,1,1), 10, 10)), LatLng)
        assert(it.isEmpty)
    }

    it("returns a tile for intersection extent in native crs") {
        val subExtent = reader.extent.copy(
            xmax = reader.extent.xmin + reader.extent.width / 100,
            ymin = reader.extent.ymax - reader.extent.height / 100)

        val tiles = reader.read(List(RasterExtent(subExtent, 10, 10)), reader.crs).toList
        info(s"tiles: ${tiles.length}")
        assert(tiles.length == 1)
    }

    it("returns a tile for intersection extent in another crs") {
        val subExtent = reader.extent.copy(
            xmax = reader.extent.xmin + reader.extent.width / 100,
            ymin = reader.extent.ymax - reader.extent.height / 100)
        val re = RasterExtent(subExtent, 10, 10)
        val tre = ReprojectRasterExtent(re, reader.crs, WebMercator).toRasterExtent
        val tiles = reader.read(List(tre), WebMercator).toList
        info(s"tiles: ${tiles.length}")
        assert(tiles.length == 1)
    }

  */
}
