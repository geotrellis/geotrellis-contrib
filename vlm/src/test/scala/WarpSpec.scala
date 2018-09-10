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

    val rasterSource = new GeoTiffRasterSource(schemeURI)

    val sourceTiff = GeoTiffReader.readMultiband(uri)

    val reprojectedRasterExtent = {
      val re = ReprojectRasterExtent(rasterSource.rasterExtent, Transform(rasterSource.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    def testReprojection(method: ResampleMethod) = {
      val warpRasterSource = rasterSource.withCRS(LatLng, method)
      val testBounds = GridBounds(0, 0, reprojectedRasterExtent.cols, reprojectedRasterExtent.rows).split(64,64).toSeq

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = reprojectedRasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(targetExtent, cols = bound.width, rows = bound.height)

          val expected: Raster[MultibandTile] = {
            val rr = implicitly[RasterRegionReproject[MultibandTile]]
            rr.regionReproject(sourceTiff.raster, sourceTiff.crs, LatLng, testRasterExtent, testRasterExtent.extent.toPolygon, method)
          }

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
