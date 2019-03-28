/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import org.scalatest.{FunSpec, Matchers}
import Timing._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.vector.{Extent, ProjectedExtent}

class SubsceneReadingIT extends FunSpec with Matchers with LazyLogging {
  val sample = "https://s3-us-west-2.amazonaws.com/radiant-nasa-iserv/2014/02/14/IP0201402141023382027S03100E/IP0201402141023382027S03100E-COG.tif"
  implicit class WithSubExtent(e: Extent) {
    /** Constructs an arbitrary subextent covering 1% of base extent. */
    def subextent: Extent = {
      val c = e.center
      val w = e.width
      val h = e.height
      Extent(c.x, c.y, c.x + w * 0.1, c.y + h * 0.1)
    }
  }
  describe("GDAL vs GeoTrellis GeoTiff reading") {
    it("should read projected extent with similar performance with GDAL") {
      val gt = time("GeoTrellis") {
        val src = GeoTiffRasterSource(sample)
        ProjectedExtent(src.extent, src.crs)
      }
      logger.info(gt._2.toString)

      val gdal = time("GDAL") {
        val src = GDALRasterSource(sample)
        ProjectedExtent(src.extent, src.crs)
      }
      logger.info(gdal._2.toString)

      gt._1 should be (gdal._1)
      gt._2.durationMillis should be(gdal._2.durationMillis +- 1000)
    }
    it("should be faster reading subextent with GDAL") {
      val gt = time("GeoTrellis") {
        val src = GeoTiffRasterSource(sample)
        // Read the tiles, forcing a mapping over cells to eliminate any chance of lazy loading
        src.read(src.extent.subextent).map(_.tile.mapDouble((_, c) => c))
      }
      logger.info(gt._2.toString)

      val gdal = time("GDAL") {
        val src = GDALRasterSource(sample)
        // Read the tiles, forcing a mapping over cells to eliminate any chance of lazy loading
        src.read(src.extent.subextent).map(_.tile.mapDouble((_, c) => c))
      }
      logger.info(gdal._2.toString)

      gt._2.durationMillis shouldBe >= (gdal._2.durationMillis)
    }
  }
}
