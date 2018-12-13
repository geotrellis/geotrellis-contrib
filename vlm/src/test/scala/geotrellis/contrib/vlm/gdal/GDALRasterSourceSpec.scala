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

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.resample._
import geotrellis.raster.testkit._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util._
import org.scalatest._
import java.net.MalformedURLException

import geotrellis.raster.io.geotiff.AutoHigherResolution

class GDALRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  val url = Resource.path("img/aspect-tiled.tif")
  println(url)
  val uri = s"file://$url"

  // we are going to use this source for resampling into weird resolutions, let's check it
  // usually we align pixels
  val source: GDALRasterSource = GDALRasterSource(uri, GDALWarpOptions(alignTargetPixels = false))

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
      source.resample(TargetRegion(expected.rasterExtent), NearestNeighbor, AutoHigherResolution)

    resampledSource should    have (dimensions (expected.tile.dimensions))

    info(s"Source CellSize: ${source.cellSize}")
    info(s"Target CellSize: ${resampledSource.cellSize}")

    // calculated expected resolutions of overviews
    // it's a rough approximation there as we're not calculating resolutions like GDAL
    val ratio = resampledSource.cellSize.resolution / source.cellSize.resolution
    resampledSource.resolutions.zip (source.resolutions.map { re =>
      val CellSize(cw, ch) = re.cellSize
      RasterExtent(re.extent, CellSize(cw * ratio, ch * ratio))
    }).map { case (rea, ree) => rea.cellSize.resolution shouldBe ree.cellSize.resolution +- 3e-1 }

    val actual: Raster[MultibandTile] =
      resampledSource.read(GridBounds(0, 0, resampledSource.cols - 1, resampledSource.rows - 1)).get

    withGeoTiffClue(actual, expected, resampledSource.crs)  {
      assertRastersEqual(actual, expected)
    }
  }

  it("should fail on creation of the GDALRasterSource on a malformed URI") {
    an[MalformedURLException] should be thrownBy GDALRasterSource("file:/random/path/here/N49W155.hgt.gz")
  }

  describe("should perform a tileToLayout") {
    val cellSizes = {
      val tiff = GeoTiffReader.readMultiband(url)
      (tiff +: tiff.overviews).map(_.rasterExtent.cellSize).map { case CellSize(w, h) =>
        CellSize(w + 1, h + 1)
      }
    }

    cellSizes.foreach { targetCellSize =>
      it(s"should perform a tileToLayout for cellSize: ${targetCellSize}") {
        val pe = ProjectedExtent(source.extent, source.crs)
        val scheme = FloatingLayoutScheme(256)
        val layout = scheme.levelFor(pe.extent, targetCellSize).layout
        val mapTransform = layout.mapTransform
        val resampledSource = source.resampleToGrid(layout)

        val expected: List[(SpatialKey, MultibandTile)] =
          mapTransform(pe.extent).coordsIter.map { spatialComponent =>
            val key = pe.translate(spatialComponent)
            val ext = mapTransform.keyToExtent(key.getComponent[SpatialKey])
            val raster = resampledSource.read(ext).get
            val newTile = raster.tile.prototype(source.cellType, layout.tileCols, layout.tileRows)
            key -> newTile.merge(
              ext,
              raster.extent,
              raster.tile,
              NearestNeighbor
            )
          }.toList

        val layoutSource = source.tileToLayout(layout)
        val actual: List[(SpatialKey, MultibandTile)] = layoutSource.readAll().toList

        withClue(s"actual.size: ${actual.size} expected.size: ${expected.size}") {
          actual.size should be(expected.size)
        }

        val sortedActual: List[Raster[MultibandTile]] =
          actual
            .sortBy { case (k, _) => (k.col, k.row) }
            .map { case (k, v) => Raster(v, mapTransform.keyToExtent(k)) }

        val sortedExpected: List[Raster[MultibandTile]] =
          expected
            .sortBy { case (k, _) => (k.col, k.row) }
            .map { case (k, v) => Raster(v, mapTransform.keyToExtent(k)) }

        val grouped: List[(Raster[MultibandTile], Raster[MultibandTile])] =
          sortedActual.zip(sortedExpected)

        grouped.foreach { case (actualTile, expectedTile) =>
          withGeoTiffClue(actualTile, expectedTile, source.crs) {
            assertRastersEqual(actualTile, expectedTile)
          }
        }
      }
    }
  }
}
