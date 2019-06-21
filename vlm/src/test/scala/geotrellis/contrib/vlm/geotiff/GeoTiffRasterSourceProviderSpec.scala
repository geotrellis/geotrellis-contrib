package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.RasterSource

import org.scalatest._


class GeoTiffRasterSourceProviderSpec extends FunSpec {
  describe("GeoTiffRasterSourceProvider") {
    val provider = new GeoTiffRasterSourceProvider()

    it("should process a non-prefixed string") {
      assert(provider.canProcess("file:///tmp/path/to/random/file.tiff"))
    }

    it("should process a prefixed string") {
      assert(provider.canProcess("gtiff+s3://my-files/tiffs/big-tiff.TIFF"))
    }

    it("should not be able to process a path that doesn't point to a GeoTiff") {
      assert(!provider.canProcess("s3://path/to/my/fav/files/cool-image.jp2"))
    }

    it("should not be able to process a GDAL prefixed path") {
      assert(!provider.canProcess("gdal+file:///tmp/temp-file.tif"))
    }

    it("should produce a GeoTiffRasterSource from a string") {
      assert(RasterSource("file://dumping-ground/part-2/random/file.tiff").isInstanceOf[GeoTiffRasterSource])
    }
  }
}
