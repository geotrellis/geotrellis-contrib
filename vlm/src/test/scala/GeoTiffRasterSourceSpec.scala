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


class GeoTiffRasterSourceSpec extends FunSpec with RasterMatchers {
  val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val schemeURI = s"file://$uri"

  val source: GeoTiffRasterSource = new GeoTiffRasterSource(schemeURI)

  it("should be able to read upper left corner") {
    val chip: Raster[MultibandTile] = source.read(GridBounds(0, 0, 10, 10)).get
  }

  it("should be able to resample") {
    val cellSize = {
      val CellSize(w, h) = source.cellSize
      CellSize(w * 2, h * 2)
    }
    val resampledSource = source.resample(source.cols / 2, source.rows / 2)
    val chip: Raster[MultibandTile] = resampledSource.read(GridBounds(0, 0, 10, 10)).get

    chip.cellSize should be (cellSize)
  }
}
