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

import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.io.geotiff._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.merge._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.io.File


class GDALRasterSourceSpec extends FunSpec with TestEnvironment {
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"

  val source = GDALRasterSource(filePath)
  val tiff = MultibandGeoTiff(filePath, streaming = true)//.mapTile(_.toArrayTile)

  describe("GDALRasterSourceSpec") {
    it("should read in the whole file correctly") {
      val actualTile = source.read(source.extent)
      val expectedTile = tiff.tile

      actualTile match {
        case Some(actual) => assertEqual(actual.tile, expectedTile)
        case None => throw new Exception(s"Could not read file at: $filePath")
      }
    }

    it("should read in the tiles correctly") {
      val gridBounds = GridBounds(0, 0, tiff.cols, tiff.rows).split(128, 128)

      for (bounds <- gridBounds) {
        val actualTile = source.read(bounds)
        val expectedTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile].crop(bounds)
        //val expectedTile = tiff.tile.crop(bounds)

        actualTile match {
          case Some(actual) => assertEqual(actual.tile, expectedTile)
          case None => throw new Exception(s"Could not read file at: $filePath")
        }
      }
    }
  }
}
