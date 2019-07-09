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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.Resource

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.util._

import org.scalatest.AsyncFunSpec

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.Duration


class GeoTiffRasterSourceMultiThreadingSpec extends AsyncFunSpec {
  val url = Resource.path("img/aspect-tiled.tif")
  val source: GeoTiffRasterSource = new GeoTiffRasterSource(url)

  implicit val ec = ExecutionContext.global

  describe("GeoTiffRasterSource should be threadsafe") {
    it("read") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        source.read()
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    it("readBounds - Option") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        source.read(source.gridBounds, 0 until source.bandCount toSeq)
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readBounds - List") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        source.readBounds(List(source.gridBounds), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readExtents") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        source.readExtents(List(source.extent), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }
  }

  describe("GeoTiffRasterReprojectSource should be threadsafe") {
    val reprojected = source.reproject(CRS.fromEpsgCode(4326))

    it("read") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        reprojected.read()
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    it("readBounds - Option") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        reprojected.read(reprojected.gridBounds, 0 until source.bandCount toSeq)
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readBounds - List") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        reprojected.readBounds(List(reprojected.gridBounds), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readExtents") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        reprojected.readExtents(List(reprojected.extent), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }
  }

  describe("GeoTiffRasterResampleSource should be threadsafe") {
    val resampled = source.resample((source.cols * 0.95).toInt , (source.rows * 0.95).toInt, NearestNeighbor)

    it("read") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        resampled.read()
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    it("readBounds - Option") {
      val res: List[Future[Option[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        resampled.read(resampled.gridBounds, 0 until source.bandCount toSeq)
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readBounds - List") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        resampled.readBounds(List(resampled.gridBounds), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }

    ignore("readExtents") {
      val res: List[Future[List[Raster[MultibandTile]]]] = (0 to 100).toList.map { _ => Future {
        resampled.readExtents(List(resampled.extent), 0 until source.bandCount toSeq).toList
      } }

      val fres: Future[List[Raster[MultibandTile]]] = Future.sequence(res).map(_.flatten)

      fres.map { rasters => assert(rasters.size == 101) }
    }
  }
}
