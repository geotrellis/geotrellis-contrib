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


class GeoTiffRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  val url = Resource.path("img/aspect-tiled.tif")

  val source: GeoTiffRasterSource = new GeoTiffRasterSource(url)

  it("should be able to read upper left corner") {
    val bounds = GridBounds(0, 0, 10, 10)
    val chip: Raster[MultibandTile] = source.read(bounds).get
    chip should have (
      dimensions (bounds.width, bounds.height),
      cellType (source.cellType)
    )
  }

  it("should not read past file edges") {
    Given("bounds larger than raster")
    val bounds = GridBounds(0, 0, source.cols + 100, source.rows + 100)
    When("reading by pixel bounds")
    val chip = source.read(bounds).get
    Then("return only pixels that exist")
    chip.tile should have (dimensions (source.dimensions))
  }

  it("should be able to resample") {
    val cellSize = {
      val CellSize(w, h) = source.cellSize
      CellSize(w * 2, h * 2)
    }

    // read in the whole file and resample the pixels in memory
    val expected: Raster[MultibandTile] =
      GeoTiffReader
        .readMultiband(url, streaming = false)
        .raster
        .resample((source.cols / 2).toInt , (source.rows / 2).toInt, NearestNeighbor)

    val resampledSource =
      source.resample(expected.tile.cols, expected.tile.rows)

    resampledSource should have (dimensions (expected.tile.dimensions))

    val actual: Raster[MultibandTile] =
      resampledSource.read(GridBounds(0, 0, resampledSource.cols+100, resampledSource.rows)).get

    withGeoTiffClue(actual, expected)  {
      assertRastersEqual(actual, expected)
    }
  }
}
