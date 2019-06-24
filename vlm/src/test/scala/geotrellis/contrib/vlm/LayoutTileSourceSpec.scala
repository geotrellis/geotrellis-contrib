/*
 * Copyright 2019 Azavea
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
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.raster.testkit.RasterMatchers

import org.scalatest._

class LayoutTileSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers {
  val testFile = Resource.path("img/aspect-tiled.tif")
  lazy val tiff = GeoTiffReader.readMultiband(testFile, streaming = false)

  lazy val rasterSource = GeoTiffRasterSource(testFile)
  val scheme = FloatingLayoutScheme(256)
  lazy val layout = scheme.levelFor(rasterSource.extent, rasterSource.cellSize).layout
  lazy val source = new LayoutTileSource(rasterSource, layout)

  val mbTestFile = Resource.path("img/multiband.tif")
  lazy val mbTiff = GeoTiffReader.readMultiband(mbTestFile, streaming = false)
  lazy val mbRasterSource = GeoTiffRasterSource(mbTestFile)
  lazy val mbLayout = scheme.levelFor(mbRasterSource.extent, mbRasterSource.cellSize).layout
  lazy val mbSource = new LayoutTileSource(mbRasterSource, mbLayout)

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

  it("should subset bands if requested") {
    val coord = mbSource.layout
      .mapTransform
      .extentToBounds(mbRasterSource.extent)
      .coordsIter.toList
      .head

    val key = SpatialKey(coord._1, coord._2)
    val re = RasterExtent(
      extent = mbLayout.mapTransform.keyToExtent(key),
      cellwidth = mbLayout.cellwidth,
      cellheight = mbLayout.cellheight,
      cols = mbLayout.tileCols,
      rows = mbLayout.tileRows)

    withClue(s"$key:") {
      val tile = mbSource.read(key, Seq(1, 2)).get
      val actual = Raster(tile, re.extent)
      val expected = Raster(
        mbTiff.crop(rasterExtent = re).tile.subsetBands(1, 2),
        re.extent
      )
      withGeoTiffClue(actual, expected, mbSource.source.crs) {
        assertRastersEqual(actual, expected)
      }
    }
  }

  describe("should read by key on high zoom levels properly // see issue-116") {
    val crs: CRS = WebMercator

    val tmsLevels: Array[LayoutDefinition] = {
      val scheme = ZoomedLayoutScheme(crs, 256)
      for (zoom <- 0 to 64) yield scheme.levelForZoom(zoom).layout
    }.toArray

    val path = Resource.path("img/issue-116-cog.tif")
    val subsetBands = List(0, 1, 2)

    val layoutDefinition = tmsLevels(22)


    for(c <- 1249656 to 1249658; r <- 1520655 to 1520658) {
      it(s"reading 22/$c/$r") {
        val result =
          GeoTiffRasterSource(path)
            .reproject(WebMercator)
            .tileToLayout(layoutDefinition)
            .read(SpatialKey(c, r), subsetBands)
            .get


        result.bands.foreach { band =>
          (1 until band.rows).foreach { r =>
            band.getDouble(band.cols - 1, r) shouldNot be(0d)
          }
        }
      }
    }
  }
}
