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
import geotrellis.contrib.vlm.gdal._
import geotrellis.raster._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import spire.syntax.cfor._
import org.scalatest._
import Inspectors._

import java.io.File

class RasterSourceRDDSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with BeforeAndAfterAll {
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val uri = s"file://$filePath"
  val rasterSource = GeoTiffRasterSource(uri)
  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  val reprojectedSource = rasterSource.reprojectToGrid(targetCRS, layout)

  describe("reading in GeoTiffs as RDDs") {
    it("should have the right number of tiles") {
      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(reprojectedSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }

      info(s"RasterSource CRS: ${reprojectedSource.crs}")

      val rdd = RasterSourceRDD(reprojectedSource, layout)


      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }

      for ((actual, expected) <- actualKeys.zip(expectedKeys)) {
        actual should be(expected)
      }
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = rasterSource.reprojectToGrid(targetCRS, layout)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)
      val rows = rdd.collect()

      forAll(rows) { case (key, tile) =>
        withClue(s"$key") {
          tile should have(
            dimensions(256, 256),
            cellType(rasterSource.cellType),
            bandCount(rasterSource.bandCount)
          )
        }
      }
    }
  }

  describe("Match reprojection from HadoopGeoTiffRDD") {
    val floatingLayout = FloatingLayoutScheme(256)
    val geoTiffRDD = HadoopGeoTiffRDD.spatialMultiband(uri)
    val md = geoTiffRDD.collectMetadata[SpatialKey](floatingLayout)._2

    val reprojectedExpectedRDD: MultibandTileLayerRDD[SpatialKey] = {
      geoTiffRDD
        .tileToLayout(md)
        .reproject(
          targetCRS,
          layout
        )._2.persist()
    }

    def assertRDDLayersEqual(
      expected: MultibandTileLayerRDD[SpatialKey],
      actual: MultibandTileLayerRDD[SpatialKey],
      matchRasters: Boolean = false
    ): Unit = {
      val joinedRDD = expected.filter { case (_, t) => !t.band(0).isNoDataTile }.leftOuterJoin(actual)

      joinedRDD.collect().foreach { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) =>
            /*writePngOutputTile(
              actual,
              name = "actual",
              discriminator = s"-${key}"
            )

            writePngOutputTile(
              expected,
              name = "expected",
              discriminator = s"-${key}"
            )*/

            // withGeoTiffClue(key, layout, actual, expected, targetCRS) {
            withClue(s"$key:") {
              expected.dimensions should be(actual.dimensions)
              if (matchRasters) assertTilesEqual(expected, actual)
            }
          // }

          case None =>
            throw new Exception(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

    it("should reproduce tileToLayout") {
      // This should be the same as result of .tileToLayout(md.layout)
      val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(rasterSource, md.layout)

      // Complete the reprojection
      val reprojectedSource = rasterSourceRDD.reproject(targetCRS, layout)._2

      assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
    }

    it("should reproduce tileToLayout followed by reproject") {
      val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
        RasterSourceRDD(rasterSource.reprojectToGrid(targetCRS, layout), layout)

      // geotrellis.raster.io.geotiff.GeoTiff(reprojectedExpectedRDD.stitch, targetCRS).write("/tmp/expected.tif")
      // geotrellis.raster.io.geotiff.GeoTiff(reprojectedSourceRDD.stitch, targetCRS).write("/tmp/actual.tif")

      val actual = reprojectedSourceRDD.stitch.tile.band(0)
      val expected = reprojectedExpectedRDD.stitch.tile.band(0)

      var (diff, pixels, mismatched) = (0d, 0d, 0)
      cfor(0)(_ < math.min(actual.cols, expected.cols), _ + 1) { c =>
        cfor(0)(_ < math.min(actual.rows, expected.rows), _ + 1) { r =>
          pixels += 1d
          if (math.abs(actual.get(c, r) - expected.get(c, r)) > 1e-6)
            diff += 1d
          if (isNoData(actual.get(c, r)) != isNoData(expected.get(c, r)))
            mismatched += 1
        }
      }

      assert(diff / pixels < 0.005) // half percent of pixels or less are not equal
      assert(mismatched < 3)
    }

    describe("GDALRasterSource") {
      val expectedFilePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled-near-merc-rdd.tif"

      it("should reproduce tileToLayout") {
        val rasterSource = GDALRasterSource(filePath)

        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(rasterSource, md.layout)
        // Complete the reprojection
        val reprojectedSource = rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource, true)
      }

      it("should reproduce tileToLayout followed by reproject GDAL") {
        val expectedRasterSource = GDALRasterSource(expectedFilePath)
        val reprojectedExpectedRDDGDAL: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(expectedRasterSource, layout)
        val rasterSource = GDALRasterSource(filePath)
        val reprojectedRasterSource = rasterSource.reprojectToGrid(targetCRS, layout)

        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(reprojectedRasterSource, layout)

        assertRDDLayersEqual(reprojectedExpectedRDDGDAL, reprojectedSourceRDD, true)
      }
    }
  }
}
