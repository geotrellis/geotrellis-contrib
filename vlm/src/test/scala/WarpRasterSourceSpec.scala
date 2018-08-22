package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
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


class WarpRasterSourceSpec extends FunSpec with TestEnvironment with RasterMatchers {
  describe("Reprojecting a RasterSource") {
    val uri = "file:///tmp/aspect-tiled.tif"
    val rasterSource = new GeoTiffRasterSource(uri)

    val sourceTiff = GeoTiffReader.readMultiband("/tmp/aspect-tiled.tif")

    val transform = Transform(rasterSource.crs, LatLng)

    val reprojectedRasterExtent = ReprojectRasterExtent(rasterSource.rasterExtent, transform)

    val cellSize = CellSize(reprojectedRasterExtent.extent, reprojectedRasterExtent.cols, reprojectedRasterExtent.rows)

    val scheme = FloatingLayoutScheme(reprojectedRasterExtent.cols, reprojectedRasterExtent.rows)
    val layout = scheme.levelFor(reprojectedRasterExtent.extent, cellSize).layout

    def testReprojection(method: ResampleMethod): Unit = {
      val expected: Raster[MultibandTile] =
        ProjectedRaster(sourceTiff.raster, rasterSource.crs)
          .reproject(
            LatLng,
            Reproject.Options(method = method)
          )

      val warpRasterSource = WarpRasterSource(rasterSource, LatLng, method)
      val rdd: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(warpRasterSource, layout)

      val actual: Raster[MultibandTile] = rdd.stitch

      val expectedTile = expected.tile.band(0)
      val actualTile = actual.tile.band(0)

      actualTile.cols should be >= (expectedTile.cols)
      actualTile.rows should be >= (expectedTile.rows)

      var errCount = 0
      val errors = collection.mutable.ListBuffer.empty[String]
      cfor(0)(_ < actual.rows, _ + 1) { row =>
        cfor(0)(_ < actual.cols, _ + 1) { col =>
          val a = actualTile.getDouble(col, row)
          if(row >= expectedTile.rows || col >= expectedTile.cols) {
            isNoData(a) should be (true)
          } else if(row != 1){
            val expected = expectedTile.getDouble(col, row)
            if (a.isNaN) {
              if (!expected.isNaN) {
                errors.append(s"Failed at col: $col and row: $row, $a != $expected\n")
                errCount += 1
              }
            } else if (expected.isNaN) {
              errors.append(s"Failed at col: $col and row: $row, $a != $expected\n")
              errCount += 1
            } else {
              if (math.abs(expected - a) > 3) {
                errors.append(s"Failed at col: $col and row: $row, $a != $expected\n")
                errCount += 1
              }
            }
          }
        }
      }

      if (errCount > 24) {
        fail("Too many pixels do not agree.  Error log follows.\n" ++ errors.reduce(_++_))
      }

      actual.extent.covers(expected.extent) should be (true)
      actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
      actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
      actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
      actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)

      assertEqual(actual, expected)
    }

    it("should reproject using NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }

    it("should reproject using Bilinear") {
      testReprojection(Bilinear)
    }
  }
}
