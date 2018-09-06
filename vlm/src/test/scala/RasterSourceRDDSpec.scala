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
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.scalatest._
import org.apache.spark._

import java.io.File


class RasterSourceRDDSpec extends FunSpec with TestEnvironment {
  val uri = s"file://${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val rasterSource = new GeoTiffRasterSource(uri)

  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  describe("reading in GeoTiffs as RDDs using GeoTiffRasterSource") {
    val uri = s"file://${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val rasterSource = new GeoTiffRasterSource(uri)

    it("should have the right number of tiles") {
      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(rasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }

      val rdd = RasterSourceRDD(rasterSource, layout)

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }

      for ((actual, expected) <- actualKeys.zip(expectedKeys)) {
        actual should be (expected)
      }
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = rasterSource.withCRS(targetCRS)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val values = rdd.values.collect()

      values.map { value => (value.cols, value.rows) should be ((256, 256)) }
    }

    /*
    it("should be the same as a tiled HadoopGeoTiffRDD") {
      val floatingLayout = FloatingLayoutScheme(256)

      val geoTiffRDD = HadoopGeoTiffRDD.spatialMultiband(uri)
      val md = geoTiffRDD.collectMetadata[SpatialKey](floatingLayout)._2
      val geoTiffTiledRDD = geoTiffRDD.tileToLayout(md)

      val reprojected: MultibandTileLayerRDD[SpatialKey] =
        geoTiffTiledRDD
          .reproject(
            targetCRS,
            layout,
            Reproject.Options(targetCellSize = Some(layout.cellSize))
          )._2

      val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] = RasterSourceRDD(rasterSource, md.layout)

      val reprojectedSource =
        rasterSourceRDD
          .reproject(
            targetCRS,
            layout,
            Reproject.Options(targetCellSize = Some(layout.cellSize))
          )._2

      val joinedRDD = reprojected.leftOuterJoin(reprojectedSource)

      joinedRDD.collect().map { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) => assertEqual(expected, actual)
          case None => throw new Exception(s"Key: key does not exist in the rasterSourceRDD")
        }
      }
    }
    */
  }

  describe("reading in GeoTiffs as RDDs using GDALRasterSource") {
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val rasterSource = GDALRasterSource(uri)

    it("should have the right number of tiles") {
      val warpRasterSource = WarpGDALRasterSource(uri, targetCRS, GDALNearestNeighbor)
      val rdd = RasterSourceRDD(warpRasterSource, layout)

      val expectedKeys =
        layout
          .mapTransform
          .keysForGeometry(warpRasterSource.extent.toPolygon)
          .toSeq
          .sortBy { key => (key.col, key.row) }.toSet

      val actualKeys = rdd.keys.collect().sortBy { key => (key.col, key.row) }.toSet

      println(s"This is the expectedKeys size: ${expectedKeys.size}")
      println(s"This is the actualKeys size: ${actualKeys.size}")

      expectedKeys should be (actualKeys)
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = WarpGDALRasterSource(uri, targetCRS, GDALNearestNeighbor)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val values = rdd.values.collect()

      values.map { value => (value.cols, value.rows) should be ((256, 256)) }
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
          layout,
          Reproject.Options(targetCellSize = Some(layout.cellSize))
        )._2.persist()
    }

    def assertRDDLayersEqual(
      expected: MultibandTileLayerRDD[SpatialKey],
      actual: MultibandTileLayerRDD[SpatialKey]
    ): Unit = {
      val joinedRDD = expected.leftOuterJoin(actual)

      joinedRDD.collect().map { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) => assertEqual(expected, actual)
          case None => throw new Exception(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

    describe("GeoTiffRasterSource") {
      val rasterSource = new GeoTiffRasterSource(uri)

      it("should reproduce tileToLayout") {
        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource, md.layout)

        // Complete the reprojection
        val reprojectedSource =
          rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
      }

      it("should reproduce tileToLayout followed by reproject") {
        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource.withCRS(targetCRS), layout)

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
      }
    }

    describe("GDALRasterSource") {
      val gdalURI = "/tmp/aspect-tiled.tif"
      val rasterSource = GDALRasterSource(gdalURI)

      it("should reproduce tileToLayout") {
        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource, md.layout)

        // Complete the reprojection
        val reprojectedSource =
          rasterSourceRDD.reproject(targetCRS, layout)._2

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
      }

      it("should reproduce tileToLayout followed by reproject") {
        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource.withCRS(targetCRS), layout)

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
      }
    }
  }
}
