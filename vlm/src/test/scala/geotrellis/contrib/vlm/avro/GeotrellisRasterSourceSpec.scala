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

import TestCatalog._

import GeotrellisRasterSource._

import org.scalatest.{FunSpec, GivenWhenThen}
import geotrellis.spark.io.{ValueReader, CollectionLayerReader}
import geotrellis.raster.{Tile, MultibandTile, RasterExtent}
import geotrellis.raster.testkit._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.raster._
import geotrellis.proj4._
import geotrellis.contrib.vlm._

import java.io.File


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
  }
}
