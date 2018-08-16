package geotrellis.contrib.vlm


import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.raster.reproject._ //Reproject.Options
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
    val uri = "file:///tmp/aspect.tif"
    val rasterSource = new GeoTiffRasterSource(uri)

    val sourceTiff = GeoTiffReader.readMultiband("/tmp/aspect.tif")

    val transform = Transform(rasterSource.crs, LatLng)
    //val sourceProjectedExtent = ProjectedExtent(rasterSource.extent, rasterSource.crs)
    //val reprojectedExtent = sourceProjectedExtent.reprojectAsPolygon(LatLng).envelope
    val reprojectedExtent = ReprojectRasterExtent.reprojectExtent(rasterSource.rasterExtent, transform)

    val cellSize = CellSize(reprojectedExtent, rasterSource.cols, rasterSource.rows)

    val scheme = FloatingLayoutScheme(rasterSource.cols, rasterSource.rows)
    val layout = scheme.levelFor(reprojectedExtent, cellSize).layout

    println(layout.tileLayout)

    def testReprojection(method: ResampleMethod): Unit = {
      val expected: Raster[MultibandTile] =
        ProjectedRaster(sourceTiff.raster, rasterSource.crs)
          .reproject(
            LatLng,
            Reproject.Options(method = method, parentGridExtent = Some(layout))
          )

      val warpRasterSource = WarpRasterSource(rasterSource, LatLng, layout, method)
      val rdd: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(warpRasterSource, layout)

      val actual: Raster[MultibandTile] = rdd.stitch

      println(expected.extent)
      println(actual.extent)

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

      //actual.extent.covers(expected.extent) should be (true)
      actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
      actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
      actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
      actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)

      //println(actual.cols, expected.cols)
      //println(actual.rows, expected.rows)

      //assertEqual(actual, expected)
    }

    it("should reproject NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }
  }
}
