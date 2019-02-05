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
import geotrellis.gdal._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.AutoHigherResolution
import geotrellis.raster.resample._
import geotrellis.raster.testkit._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util._
import org.scalatest._

import java.net.MalformedURLException

class GDALConvertedRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  val url = Resource.path("img/aspect-tiled.tif")
  val uri = s"file://$url"

  // we are going to use this source for resampling into weird resolutions, let's check it
  // usually we align pixels
  val source: GDALRasterSource = GDALRasterSource(uri, GDALWarpOptions(alignTargetPixels = true))

  val expectedRaster: Raster[MultibandTile] =
    GeoTiffReader
      .readMultiband(url, streaming = false)
      .raster

  val targetExtent = expectedRaster.gridBounds//extent

  val expectedTile: MultibandTile = expectedRaster.tile

  describe("Converting to a different CellType") {
    describe("Byte CellType") {
      it("should convert to: ByteConstantNoDataCellType") {
        val actual: Raster[MultibandTile] = source.convert(ByteConstantNoDataCellType).read(targetExtent).get

        val expected: Raster[MultibandTile] =
          expectedRaster.copy(tile = expectedTile.convert(ByteConstantNoDataCellType).withNoData(Some(Byte.MinValue)))

        //writePngOutputTile(actual.tile, name = "actual-result", outputDir = Some("/tmp"))
        //writePngOutputTile(expected.tile, name = "expected-result", outputDir = Some("/tmp"))

        assertRastersEqual(actual, expected, 1.0)
        //assertEqual(actual, expected)
      }
    }
  }
}
