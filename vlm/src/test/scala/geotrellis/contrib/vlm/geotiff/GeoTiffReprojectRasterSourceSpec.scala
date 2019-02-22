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
 
package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import org.scalatest._
import java.io.File

import geotrellis.spark.tiling.LayoutDefinition

class GeoTiffReprojectRasterSourceSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  describe("Reprojecting a RasterSource") {
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val schemeURI = s"file://$uri"

    val rasterSource = GeoTiffRasterSource(schemeURI)
    val sourceTiff = GeoTiffReader.readMultiband(uri)
    
    val expectedRasterExtent = {
      val re = ReprojectRasterExtent(rasterSource.layerGridExtent, Transform(rasterSource.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    def testReprojection(method: ResampleMethod) = {
      val warpRasterSource = rasterSource.reprojectToRegion(LatLng, expectedRasterExtent, method)

      warpRasterSource.resolutions.size shouldBe rasterSource.resolutions.size
      
      val testBounds = GridBounds(0, 0, expectedRasterExtent.cols, expectedRasterExtent.rows).split(64,64).toSeq

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = expectedRasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(
            extent     = targetExtent,
            cellwidth  = expectedRasterExtent.cellwidth,
            cellheight = expectedRasterExtent.cellheight, 
            cols       = bound.width,
            rows       = bound.height
          )

          val expected: Raster[MultibandTile] = {
            val rr = implicitly[RasterRegionReproject[MultibandTile]]
            rr.regionReproject(sourceTiff.raster, sourceTiff.crs, LatLng, testRasterExtent, testRasterExtent.extent.toPolygon, method)
          }

          val actual = warpRasterSource.read(bound).get

          actual.extent.covers(expected.extent) should be (true)
          actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
          actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
          actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
          actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)

          withGeoTiffClue(actual, expected, LatLng)  {
            assertRastersEqual(actual, expected)
          }
        }
      }
    }

    it("should reproject using NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }

    it("should reproject using Bilinear") {
      testReprojection(Bilinear)
    }

    it("should select correct overview to sample from") {
      // we choose LatLng to switch scales, the source projection is in meters
      val baseReproject = rasterSource.reproject(LatLng).asInstanceOf[GeoTiffReprojectRasterSource]
      // known good start, CellSize(10, 10) is the base resolution of source
      baseReproject.closestTiffOverview.cellSize shouldBe CellSize(10, 10)

      info(s"lcc resolutions: ${rasterSource.resolutions.map(_.cellSize)}")
      val twiceFuzzyLayout = {
        val CellSize(width, height) = baseReproject.cellSize
        LayoutDefinition(RasterExtent(LatLng.worldExtent, CellSize(width*2.1, height*2.1)), tileSize = 256)
      }

      val twiceFuzzySource = rasterSource.reprojectToGrid(LatLng, twiceFuzzyLayout).asInstanceOf[GeoTiffReprojectRasterSource]
      twiceFuzzySource.closestTiffOverview.cellSize shouldBe CellSize(20,20)

      val thriceFuzzyLayout = {
        val CellSize(width, height) = baseReproject.cellSize
        LayoutDefinition(RasterExtent(LatLng.worldExtent, CellSize(width*3.5, height*3.5)), tileSize = 256)
      }

      val thriceFuzzySource = rasterSource.reprojectToGrid(LatLng, thriceFuzzyLayout).asInstanceOf[GeoTiffReprojectRasterSource]
      thriceFuzzySource.closestTiffOverview.cellSize shouldBe CellSize(20,20)

      val quatroFuzzyLayout = {
        val CellSize(width, height) = baseReproject.cellSize
        LayoutDefinition(RasterExtent(LatLng.worldExtent, CellSize(width*4.1, height*4.1)), tileSize = 256)
      }

      val quatroTimesFuzzySource = rasterSource.reprojectToGrid(LatLng, quatroFuzzyLayout).asInstanceOf[GeoTiffReprojectRasterSource]
      quatroTimesFuzzySource.closestTiffOverview.cellSize shouldBe CellSize(40.0,39.94082840236686)

    }
  }
}
