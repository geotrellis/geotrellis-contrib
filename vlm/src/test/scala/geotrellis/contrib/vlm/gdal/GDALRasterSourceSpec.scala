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
import geotrellis.raster.prototype._
import geotrellis.raster.merge._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.raster.testkit._
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util._

import org.scalatest._

class GDALRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  val url = Resource.path("img/aspect-tiled.tif")

  val source: GDALRasterSource = GDALRasterSource(url)

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
    val expected = source.read(source.extent)

    Then("return only pixels that exist")
    chip.tile should have (dimensions (source.dimensions))

    // check also that the tile is valid
    withGeoTiffClue(chip, expected.get, source.crs)  {
      assertRastersEqual(chip, expected.get)
    }
  }

  // no resampling is implemented there
  it("should be able to resample") {
    // read in the whole file and resample the pixels in memory
    val expected: Raster[MultibandTile] =
      GeoTiffReader
        .readMultiband(url, streaming = false)
        .raster
        .resample((source.cols * 0.95).toInt , (source.rows * 0.95).toInt, NearestNeighbor)
    // resample to 0.9 so we RasterSource picks the base layer and not an overview

    val resampledSource =
      source.resample(expected.tile.cols, expected.tile.rows, NearestNeighbor)

    resampledSource should have (dimensions (expected.tile.dimensions))

    val actual: Raster[MultibandTile] =
      resampledSource.read(GridBounds(0, 0, resampledSource.cols - 1, resampledSource.rows - 1)).get

    withGeoTiffClue(actual, expected, resampledSource.crs)  {
      assertRastersEqual(actual, expected)
    }
  }

  it("should perform a tileToLayout") {
    val targetCellSize: CellSize = CellSize(20.0, 20.0)
    val targetExtent: Extent = source.extent.buffer(10.0)

    val scheme = FloatingLayoutScheme(256)
    val layout = scheme.levelFor(targetExtent, targetCellSize).layout
    val mapTransform = layout.mapTransform

    val testSource: MultibandGeoTiff =
      GeoTiffReader.readMultiband(url, streaming = false)

    val testTile = testSource.tile

    val pe = ProjectedExtent(testSource.extent, testSource.crs)

    val expected: List[(SpatialKey, MultibandTile)] =
      mapTransform(pe.extent)
        .coordsIter.map { spatialComponent =>
          val key = pe.translate(spatialComponent)
          val newTile = testTile.prototype(source.cellType, layout.tileCols, layout.tileRows)
          (key,
            newTile.merge(
              mapTransform.keyToExtent(key.getComponent[SpatialKey]),
              pe.extent,
              testTile,
              NearestNeighbor
            )
          )
        }.toList

    val actual: List[(SpatialKey, MultibandTile)] = source.tileToLayout(layout).readAll().toList

    println(s"This is the count for expected: ${expected.size}")
    println(s"This is the count for actual: ${actual.size}")

    actual.size should be (expected.size)

    println("\nThese are the expected keys")
    expected.sortBy { case (key, _) => (key.col, key.row) }.foreach { case (k, _) => println(k) }
    println("\nThese are the actual keys")
    actual.sortBy { case (key, _) => (key.col, key.row) }.foreach { case (k, _) => println(k) }

    val grouped: List[(Raster[MultibandTile], Raster[MultibandTile])] =
      actual
        .sortBy { case (k, _) => (k.col, k.row) }
        .map { case (k, v) => Raster(v, mapTransform.keyToExtent(k)) }
        .zip(
          expected
            .sortBy { case (k, _) => (k.col, k.row) }
            .map { case (k, v) => Raster(v, mapTransform.keyToExtent(k)) }
        )

    grouped.map { case (actualTile, expectedTile) =>
      withGeoTiffClue(actualTile, expectedTile, source.crs)  {
        assertRastersEqual(actualTile, expectedTile)
      }
    }
  }
}
