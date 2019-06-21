package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.RasterSource

import org.scalatest._


class GDALRasterSourceProviderSpec extends FunSpec {
  describe("GDALRasterSourceProvider") {
    val provider = new GDALRasterSourceProvider()

    it("should process a VSI path") {
      assert(provider.canProcess("/vsicurl/https://path/to/some/file.tiff"))
    }

    it("should process a non-prefixed string") {
      assert(provider.canProcess("s3://bucket/thing/something/file.jp2"))
    }

    it("should process a prefixed string") {
      assert(provider.canProcess("zip+s3://bucket/thing/something/more-data.zip"))
    }

    it("should not be able to process a GeoTrellis catalog path") {
      assert(!provider.canProcess("s3://path/to/my/fav/catalog?layer=fav&zoom=3"))
    }

    it("should not be able to process a GeoTiff prefixed path") {
      assert(!provider.canProcess("gtiff+file:///tmp/temp-file.tif"))
    }

    it("should produce a GDALRasterSource from a string") {
      assert(RasterSource("file:///tmp/dumping-ground/random/file.zip").isInstanceOf[GDALRasterSource])
    }
  }
}
