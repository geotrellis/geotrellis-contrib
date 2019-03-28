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

import geotrellis.contrib.vlm.geotiff._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{IntConstantNoDataArrayTile, MultibandTile, Raster}
import geotrellis.vector.Extent

import cats.data.NonEmptyList
import org.scalatest._

class MosaicRasterSourceSpec extends FunSpec with Matchers {

  describe("union operations") {
    // With Extent(0, 0, 1, 1)
    val inputPath1 = Resource.path("img/geotiff-at-origin.tif")
    // With Extent(1, 0, 2, 1)
    val inputPath2 = Resource.path("img/geotiff-off-origin.tif")

    val gtRasterSource1 = GeoTiffRasterSource(inputPath1)
    val gtRasterSource2 = GeoTiffRasterSource(inputPath2)

    val mosaicRasterSource = MosaicRasterSource(
      NonEmptyList(gtRasterSource1, List(gtRasterSource2)), LatLng,
      gtRasterSource1.gridExtent combine gtRasterSource2.gridExtent)

    it("should understand its bounds") {
      mosaicRasterSource.cols shouldBe 8
      mosaicRasterSource.rows shouldBe 4
    }

    it("should union extents of its sources") {
      mosaicRasterSource.gridExtent shouldBe (
        gtRasterSource1.gridExtent combine gtRasterSource2.gridExtent
      )
    }

    it("should union extents with reprojection") {
      mosaicRasterSource.reproject(WebMercator).gridExtent shouldBe (
        mosaicRasterSource.gridExtent.reproject(LatLng, WebMercator)
      )
    }

    it("should return the whole tiles from the whole tiles' extents") {
      val extentRead1 = Extent(0, 0, 1, 1)
      val extentRead2 = Extent(1, 0, 2, 1)
      mosaicRasterSource.read(extentRead1, Seq(0)) shouldBe
        gtRasterSource1.read(gtRasterSource1.gridExtent.extent, Seq(0))
      mosaicRasterSource.read(extentRead2, Seq(0)) shouldBe
        gtRasterSource2.read(gtRasterSource2.gridExtent.extent, Seq(0))
    }

    it("should read an extent overlapping both tiles") {
      val extentRead = Extent(0, 0, 1.5, 1)
      val expectation = Raster(
        MultibandTile(
          IntConstantNoDataArrayTile(Array(1, 2, 3, 4, 1, 2,
                                           5, 6, 7, 8, 5, 6,
                                           9, 10, 11, 12, 9, 10,
                                           13, 14, 15, 16, 13, 14),
                                     6, 4)),
        extentRead
      )
      val result = mosaicRasterSource.read(extentRead, Seq(0)).get
      result shouldEqual expectation
    }

    it("should get the expected tile from a gridbounds-based read") {
      val expectation = Raster(
        MultibandTile(
          IntConstantNoDataArrayTile(Array(1, 2, 3, 4, 1, 2, 3, 4,
                                           5, 6, 7, 8, 5, 6, 7, 8,
                                           9, 10, 11, 12, 9, 10, 11, 12,
                                           13, 14, 15, 16, 13, 14, 15, 16),
                                     8, 4)),
          mosaicRasterSource.gridExtent.extent
      )
      val result = mosaicRasterSource.read(mosaicRasterSource.gridBounds, Seq(0)).get
      result shouldEqual expectation
      result.extent shouldEqual expectation.extent
    }
  }
}
