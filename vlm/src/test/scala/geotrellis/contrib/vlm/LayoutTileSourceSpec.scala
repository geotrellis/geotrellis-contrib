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
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.io.geotiff.reader._

import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._
import geotrellis.raster.testkit.RasterMatchers

import org.scalatest._

class LayoutTileSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers {
  val testFile = Resource.path("img/aspect-tiled.tif")
  val tiff = GeoTiffReader.readMultiband(testFile, streaming = false)

  val rasterSource = new GeoTiffRasterSource(testFile)
  val scheme = FloatingLayoutScheme(256)
  val layout = scheme.levelFor(rasterSource.extent, rasterSource.cellSize).layout
  val source = new LayoutTileSource(rasterSource, layout)


  it("should read all the keys") {
    val keys = source.layout
      .mapTransform
      .extentToBounds(rasterSource.extent)
      .coordsIter.toList

    for ((col, row) <- keys ) {
      val key = SpatialKey(col, row)
      val re = RasterExtent(
        extent = layout.mapTransform.keyToExtent(key),
        cellwidth = layout.cellwidth,
        cellheight = layout.cellheight,
        cols = layout.tileCols,
        rows = layout.tileRows)

      withClue(s"$key:") {
        val tile = source.read(key).get
        val actual = Raster(tile, re.extent)
        val expected = tiff.crop(rasterExtent = re)

        withGeoTiffClue(actual, expected, source.source.crs) {
          assertRastersEqual(actual, expected)
        }
      }
    }
  }
}
