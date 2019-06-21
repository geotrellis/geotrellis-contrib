package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.RasterSource

import org.scalatest._


class GeoTrellisRasterSourceProviderSpec extends FunSpec {
  describe("GeoTrellisRasterSourceProvider") {
    val provider = new GeoTrellisRasterSourceProvider()

    it("should process a non-prefixed string") {
      assert(provider.canProcess("hdfs://storage/big-catalog?layer=big&zoom=30"))
    }

    it("should process a prefixed string") {
      assert(provider.canProcess("gt+s3://catalog/path/blah?layer=blah!&zoom=0&band_count=4"))
    }

    it("should not be able to process a path that doesn't contain a layer name") {
      assert(!provider.canProcess("file://this/path/leads/to/a/bad/catalog"))
    }

    it("should not be able to process a path that doesn't point to a catalog") {
      assert(!provider.canProcess("s3://path/to/my/fav/files/cool-image-3.jp2"))
    }

    it("should not be able to process a GDAL prefixed path") {
      assert(!provider.canProcess("gdal+file:///sketch-pad/files/temp-file.tif"))
    }

    it("should produce a GeoTrellisRasterSource from a string") {
      assert(RasterSource("s3://fast/bulbous/catalog?layer=fast&zoom=10").isInstanceOf[GeotrellisRasterSource])
    }
  }
}
