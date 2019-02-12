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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{Auto, AutoHigherResolution, Base}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.testkit._
import geotrellis.raster.{MultibandTile, RasterExtent}
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark._
import geotrellis.spark.io.CollectionLayerReader
import geotrellis.vector.Extent

import org.scalatest.{FunSpec, GivenWhenThen}

class GeotrellisRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen with CatalogTestEnvironment {
  val uriMultiband = s"file://${TestCatalog.multibandOutputPath}"
  val uriSingleband = s"file://${TestCatalog.singlebandOutputPath}"
  val layerId = LayerId("landsat", 0)
  val sourceMultiband = GeotrellisRasterSource(uriMultiband, layerId)
  val sourceSingleband = GeotrellisRasterSource(uriSingleband, layerId)

  describe("geotrellis raster source") {

    it("should read singleband tile") {
      val bounds = GridBounds(0, 0, 2, 2)
      // NOTE: All tiles are converted to multiband
      val chip: Raster[MultibandTile] = sourceSingleband.read(bounds).get
      chip should have (
        dimensions (bounds.width, bounds.height),
        cellType (sourceSingleband.cellType)
      )
    }

    it("should read multiband tile") {
      val bounds = GridBounds(0, 0, 2, 2)
      val chip: Raster[MultibandTile] = sourceMultiband.read(bounds).get
      chip should have (
        dimensions (bounds.width, bounds.height),
        cellType (sourceMultiband.cellType)
      )
    }

    it("should read offset tile") {
      val bounds = GridBounds(2, 2, 4, 4)
      val chip: Raster[MultibandTile] = sourceMultiband.read(bounds).get
      chip should have (
        dimensions (bounds.width, bounds.height),
        cellType (sourceMultiband.cellType)
      )
    }

    it("should read entire file") {
      val bounds = GridBounds(0, 0, sourceMultiband.cols - 1, sourceMultiband.rows - 1)
      val chip: Raster[MultibandTile] = sourceMultiband.read(bounds).get
      chip should have (
        dimensions (sourceMultiband.dimensions),
        cellType (sourceMultiband.cellType)
      )
    }

    it("should not read past file edges") {
      Given("bounds larger than raster")
      val bounds = GridBounds(0, 0, sourceMultiband.cols + 100, sourceMultiband.rows + 100)
      When("reading by pixel bounds")
      val chip = sourceMultiband.read(bounds).get
      Then("return only pixels that exist")
      chip.tile should have (dimensions (sourceMultiband.dimensions))
    }

    it("should be able to read empty layer") {
      val bounds = GridBounds(9999, 9999, 10000, 10000)
      assert(sourceMultiband.read(bounds) == None)
    }

    it("should be able to resample") {
      // read in the whole file and resample the pixels in memory
      val expected: Raster[MultibandTile] =
        GeoTiffReader
          .readMultiband(TestCatalog.filePath, streaming = false)
          .raster
          .resample((sourceMultiband.cols * 0.95).toInt, (sourceMultiband.rows * 0.95).toInt, NearestNeighbor)
          // resample to 0.9 so RasterSource picks the base layer and not an overview

      val resampledSource =
        sourceMultiband.resample(expected.tile.cols, expected.tile.rows, NearestNeighbor)

      resampledSource should have (dimensions (expected.tile.dimensions))

      val actual: Raster[MultibandTile] =
        resampledSource
          .resampleToGrid(expected.rasterExtent)
          .read(expected.extent)
          .get

      withGeoTiffClue(actual, expected, resampledSource.crs)  {
        assertRastersEqual(actual, expected)
      }
    }

    it("should have resolutions only for given layer name") {
      assert(
        sourceMultiband.resolutions.length ===
          CollectionLayerReader(uriMultiband).attributeStore.layerIds.filter(_.name == layerId.name).length
      )
      assert(
        GeotrellisRasterSource(uriMultiband, LayerId("bogusLayer", 0)).resolutions.length === 0
      )
    }

    it("should get the closest resolution") {
      val extent = Extent(0.0, 0.0, 10.0, 10.0)
      val rasterExtent1 = RasterExtent(extent, 1.0, 1.0, 10, 10)
      val rasterExtent2 = RasterExtent(extent, 2.0, 2.0, 10, 10)
      val rasterExtent3 = RasterExtent(extent, 4.0, 4.0, 10, 10)

      val resolutions = List(rasterExtent1, rasterExtent2, rasterExtent3)
      val cellSize1 = CellSize(1.0, 1.0)
      val cellSize2 = CellSize(2.0, 2.0)

      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, AutoHigherResolution).get == rasterExtent1)
      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize2, AutoHigherResolution).get == rasterExtent2)

      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, Auto(0)).get == rasterExtent1)
      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, Auto(1)).get == rasterExtent2)
      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, Auto(2)).get == rasterExtent3)
      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, Auto(3)) == None)

      assert(GeotrellisRasterSource.getClosestResolution(resolutions, cellSize1, Base) == None)
    }

    it("should get the closest layer") {
      val extent = Extent(0.0, 0.0, 10.0, 10.0)
      val rasterExtent1 = RasterExtent(extent, 1.0, 1.0, 10, 10)
      val rasterExtent2 = RasterExtent(extent, 2.0, 2.0, 10, 10)
      val rasterExtent3 = RasterExtent(extent, 4.0, 4.0, 10, 10)

      val resolutions = List(rasterExtent1, rasterExtent2, rasterExtent3)

      val layerId1 = LayerId("foo", 0)
      val layerId2 = LayerId("foo", 1)
      val layerId3 = LayerId("foo", 2)
      val layerIds = List(layerId1, layerId2, layerId3)

      val cellSize = CellSize(1.0, 1.0)

      assert(GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, layerId3, cellSize) == layerId1)
      assert(GeotrellisRasterSource.getClosestLayer(List(), List(), layerId3, cellSize) == layerId3)
      assert(GeotrellisRasterSource.getClosestLayer(resolutions, List(), layerId3, cellSize) == layerId3)
    }

    it("should reproject") {
      val targetCRS = WebMercator
      val bounds = GridBounds(0, 0, sourceMultiband.cols - 1, sourceMultiband.rows - 1)

      val expected: Raster[MultibandTile] =
        GeoTiffReader
          .readMultiband(TestCatalog.filePath, streaming = false)
          .raster
          .reproject(bounds, sourceMultiband.crs, targetCRS)

      val reprojectedSource = sourceMultiband.reprojectToRegion(targetCRS, expected.rasterExtent)

      reprojectedSource should have (dimensions (expected.tile.dimensions))

      val actual: Raster[MultibandTile] =
        reprojectedSource
          .reprojectToRegion(targetCRS, expected.rasterExtent)
          .read(expected.rasterExtent.extent)
          .get

      withGeoTiffClue(actual, expected, reprojectedSource.crs)  {
        assertRastersEqual(actual, expected)
      }
    }
  }
}
