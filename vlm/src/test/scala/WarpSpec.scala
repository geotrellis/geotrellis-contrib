package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.raster.testkit._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.stitch._
import geotrellis.spark.testkit._

import org.scalatest._

import org.apache.spark._

import spire.syntax.cfor._


class WarpSpec extends FunSpec with TestEnvironment with RasterMatchers {
  describe("Reprojecting a RasterSource") {
    val uri = "file:///tmp/aspect-tiled.tif"


    val rasterSource = new GeoTiffRasterSource(uri)

    val sourceTiff = GeoTiffReader.readMultiband("/tmp/aspect-tiled.tif")

    val reprojectedRasterExtent = {
      val re = ReprojectRasterExtent(rasterSource.rasterExtent, Transform(rasterSource.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    def testReprojection(method: ResampleMethod) = {
      val warpRasterSource = WarpGeoTiffRasterSource(uri, LatLng, method)
      val testBounds = GridBounds(0, 0, reprojectedRasterExtent.cols, reprojectedRasterExtent.rows).split(64,64).toSeq

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = reprojectedRasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(targetExtent, bound.width, bound.height)

          val expected: Raster[MultibandTile] = sourceTiff.raster.reproject(
            targetRasterExtent = testRasterExtent,
            transform = Transform(rasterSource.crs, LatLng),
            inverseTransform = Transform(LatLng, rasterSource.crs))

          val actual = warpRasterSource.read(List(testRasterExtent)).next

          val expectedTile = expected.tile.band(0)
          val actualTile = actual.tile.band(0)

          actual.extent.covers(expected.extent) should be (true)
          actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
          actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
          actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
          actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)
          assertEqual(actual, expected)
        }
      }
    }

    it("should reproject using NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }

    it("should reproject using Bilinear") {
      testReprojection(Bilinear)
    }
  }
}
