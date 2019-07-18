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
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.vector.Extent
import geotrellis.proj4.CRS

import cats._
import cats.syntax.parallel._
import cats.syntax.traverse._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.option._
import cats.instances.list._
import cats.instances.future._
import cats.data.NonEmptyList
import cats.temp.par._
import cats.effect._
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class GeoTiffRasterSourceMultiThreadingSpec extends AsyncFunSpec with Matchers {
  lazy val uri = geotrellis.contrib.vlm.Resource.path("img/aspect-tiled.tif")

  implicit val ec = ExecutionContext.global
  implicit lazy val cs = IO.contextShift(ec)

  val iterations = (0 to 30).toList

  def testMultithreading[F[_]: Monad: Par](rs: RasterSourceF[F], assertion: F[List[Raster[MultibandTile]]] => Future[compatible.Assertion]): Unit = {
    it("read") {
      val res = iterations.map { _ => rs.read() }.parSequence
      assertion(res)
    }

    it("readBounds") {
      val res =
        iterations
          .map { _ => (rs.gridBounds, rs.bandCount).mapN { case (gb, bc) => rs.read(gb, 0 until bc) } }
          .parSequence
          .map(_.parSequence)
          .flatten

      assertion(res)
    }
  }

  def testMultithreadingFuture(rs: RasterSourceF[Future], assertion: Future[List[Raster[MultibandTile]]] => Future[compatible.Assertion]): Unit = {
    it("read") {
      val res = iterations.map { _ => rs.read() }.sequence
      assertion(res)
    }

    it("readBounds") {
      val res =
        iterations
          .map { _ => (rs.gridBounds, rs.bandCount).mapN { case (gb, bc) => rs.read(gb, 0 until bc) } }
          .map(_.flatten)
          .sequence

      assertion(res)
    }
  }

  describe("GeoTiffRasterSourcesF should work correctly with different types of effects") {
    describe("Option effect") {
      it("Should read different extents properly") {
        lazy val source: GeoTiffRasterSource[Option] = GeoTiffRasterSource[Option](uri)

        def raster1: Option[Raster[MultibandTile]] = source.read(Extent(0, 0, 1, 1))
        def raster2: Option[Raster[MultibandTile]] = source.read(Extent(630000.0, 215000.0, 639000.0, 219500.0))
        def raster3: Option[Raster[MultibandTile]] = source.read()
        def run = List(raster1, raster2, raster3)

        run.tail shouldNot contain(None)
      }
    }

    describe("IO effect") {
      lazy val source: GeoTiffRasterSource[IO] = GeoTiffRasterSource[IO](uri)

      describe("GeoTiffRasterSource should be threadsafe") {
        testMultithreading[IO](source, _.unsafeToFuture.map(_.length shouldBe iterations.length))
      }

      describe("GeoTiffRasterReprojectSource should be threadsafe") {
        testMultithreading[IO](
          source.reproject(CRS.fromEpsgCode(4326)),
          _.unsafeToFuture.map(_.length shouldBe iterations.length)
        )
      }

      describe("GeoTiffRasterResampleSource should be threadsafe") {
        val (cols, rows) = (source.cols, source.rows).mapN { case (c, r) => (c * 0.95).toInt -> (r * 0.95).toInt }.unsafeRunSync()
        testMultithreading[IO](
          source.resample(cols, rows, NearestNeighbor),
          _.unsafeToFuture.map(_.length shouldBe iterations.length)
        )
      }

      describe("MosaicRasterSource should be threadsafe") {
        lazy val source = GeoTiffRasterSource[IO](uri)
        lazy val rasterSources = IO(NonEmptyList(GeoTiffRasterSource[IO](uri), List(GeoTiffRasterSource[IO](uri), GeoTiffRasterSource[IO](uri))))
        lazy val msource = MosaicRasterSource[IO](rasterSources, source.crs)

        testMultithreading[IO](msource, _.unsafeToFuture.map(_.length shouldBe iterations.length))
      }
    }

    describe("Future effect") {
      lazy val source: GeoTiffRasterSource[Future] = GeoTiffRasterSource[Future](uri)

      describe("GeoTiffRasterSource should be threadsafe") {
        testMultithreadingFuture(source, _.map(_.length shouldBe iterations.length))
      }

      describe("GeoTiffRasterReprojectSource should be threadsafe") {
        testMultithreadingFuture(
          source.reproject(CRS.fromEpsgCode(4326)),
          _.map(_.length shouldBe iterations.length)
        )
      }

      describe("GeoTiffRasterResampleSource should be threadsafe") {
        val (cols, rows) = Await.result((source.cols, source.rows).mapN { case (c, r) => (c * 0.95).toInt -> (r * 0.95).toInt }, Duration.Inf)
        testMultithreadingFuture(
          source.resample(cols, rows, NearestNeighbor),
          _.map(_.length shouldBe iterations.length)
        )
      }
    }
  }
}
