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

package geotrellis.contrib.vlm.effect

import geotrellis.contrib.vlm.effect.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.Extent

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

class GeoTiffRasterSourceSpec extends AsyncFunSpec with Matchers {
  lazy val uri = geotrellis.contrib.vlm.Resource.path("img/aspect-tiled.tif")

  implicit val ec = ExecutionContext.global
  implicit lazy val cs = IO.contextShift(ec)

  describe("GeoTiffRasterSourcesF should work correctly with different types of effects") {

    it("Option effect") {
      lazy val source: GeoTiffRasterSource[Option] = GeoTiffRasterSource[Option](uri)

      def raster1: Option[Raster[MultibandTile]] = source.read(Extent(0, 0, 1, 1))
      def raster2: Option[Raster[MultibandTile]] = source.read(Extent(630000.0, 215000.0, 639000.0, 219500.0))
      def raster3: Option[Raster[MultibandTile]] = source.read()
      def run = List(raster1, raster2, raster3)

      run.tail shouldNot contain (None)
    }

    it("IO effect") {
      lazy val source: GeoTiffRasterSource[IO] = GeoTiffRasterSource[IO](uri)

      val res =
        (0 to 20)
          .toList
          .map { _ => source.read() }
          .parSequence
          .attempt
          .unsafeRunSync()

      res.isRight shouldBe true
    }

    it("IO effect (MosaicRasterSource)") {
      lazy val source: GeoTiffRasterSource[IO] = GeoTiffRasterSource[IO](uri)
      lazy val rasterSources = IO(NonEmptyList(GeoTiffRasterSource[IO](uri), List(GeoTiffRasterSource[IO](uri), GeoTiffRasterSource[IO](uri))))

      lazy val msource = MosaicRasterSource[IO](rasterSources, source.crs)

      val mres =
        (0 to 20)
          .toList
          .map { _ => msource.read() }
          .parSequence
          .attempt
          .unsafeRunSync()

      mres.isRight shouldBe true
    }

    it("Future effect") {
      // since IO defers evaluation and TIFF fetch
      // Future is the only simple way to check RasterSources
      // for the agressive data races
      val rs = GeoTiffRasterSource[Future](uri)
      val fres = Future.sequence((0 to 20).toList.map { _ => rs.read() })
      fres.map { list => list.length shouldBe 21 }
    }
  }
}
