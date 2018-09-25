/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.raster.testkit._
import geotrellis.proj4._
import geotrellis.spark.testkit._
import org.scalatest._

import java.io.File


class WarpSpec extends FunSpec with TestEnvironment with RasterMatchers {
  describe("Reprojecting a RasterSource") {
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val schemeURI = s"file://$uri"

    val sourceTiff = GeoTiffReader.readMultiband(uri)

    val expectedRasterExtent = {
      val re = ReprojectRasterExtent(sourceTiff.rasterExtent, Transform(sourceTiff.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    def testReprojection(rasterSource: RasterSource, method: ResampleMethod) = {
      val warpRasterSource = rasterSource.reproject(LatLng, method, expectedRasterExtent)

      val testBounds = GridBounds(0, 0, expectedRasterExtent.cols, expectedRasterExtent.rows).split(64,64).toSeq

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = expectedRasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(
            extent = targetExtent,
            cellwidth = expectedRasterExtent.cellwidth,
            cellheight = expectedRasterExtent.cellheight,
            cols = bound.width, rows = bound.height)

          val expected: Raster[MultibandTile] = {
            val rr = implicitly[RasterRegionReproject[MultibandTile]]
            rr.regionReproject(sourceTiff.raster, sourceTiff.crs, LatLng, testRasterExtent, testRasterExtent.extent.toPolygon, method)
          }

          val actual = warpRasterSource.read(bound).get

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

    describe("WarpGeoTiffRasterSource") {
      val rasterSource = GeoTiffRasterSource(schemeURI)

      it("should reproject using NearestNeighbor") {
        testReprojection(rasterSource, NearestNeighbor)
      }

      it("should reproject using Bilinear") {
        testReprojection(rasterSource, Bilinear)
      }
    }

    describe("WarpGDALRasterSource") {
      val rasterSource = GDALRasterSource(uri)

      it("should reproject using NearestNeighbor") {
        testReprojection(rasterSource, NearestNeighbor)
      }

      it("should reproject using Bilinear") {
        testReprojection(rasterSource, Bilinear)
      }
    }
  }
}
