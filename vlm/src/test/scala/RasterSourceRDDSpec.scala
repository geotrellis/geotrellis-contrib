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
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val uri = s"file://$filePath"
  val rasterSource = new GeoTiffRasterSource(uri)

  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  describe("reading in GeoTiffs as RDDs") {
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
        actual should be(expected)
      }
    }

    it("should read in the tiles as squares") {
      val reprojectedRasterSource = rasterSource.reproject(targetCRS)
      val rdd = RasterSourceRDD(reprojectedRasterSource, layout)

      val values = rdd.values.collect()

      values.map { value => (value.cols, value.rows) should be((256, 256)) }
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
      val joinedRDD = expected.filter{ case (_, t) => !t.band(0).isNoDataTile }.leftOuterJoin(actual)

      joinedRDD.collect().map { case (key, (expected, actualTile)) =>
        actualTile match {
          case Some(actual) =>
            // println(s"actual.dimensions: ${actual.dimensions}")
            // println(s"expected.dimensions: ${expected.dimensions}")
            expected.band(0).renderPng().write(s"/Users/daunnc/Downloads/expected-${key}.png")
            actual.band(0).renderPng().write(s"/Users/daunnc/Downloads/actual-${key}.png")
            //if(key == SpatialKey(2305,3223)) assertEqual(expected, actual)
          case None =>
            expected.band(0).renderPng().write(s"/Users/daunnc/Downloads/expected-${key}.png")
            println(s"$key does not exist in the rasterSourceRDD")
        }
      }
    }

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
        RasterSourceRDD(
          rasterSource.reproject(
            targetCRS,
            options = Reproject.Options.DEFAULT.copy(
            //targetCellSize = Some(CellSize(19.2, 19.2))
            targetCellSize = Some(layout.cellSize)
            )
          ),
          layout
        )

      assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
    }

    describe("GDALRasterSource ONLY") {
      val rasterSource = GDALRasterSource(filePath)

      println("==========")
      println(s"layout.cellSize: ${layout.cellSize}")
      println("==========")

      ignore("should reproduce tileToLayout ONLY") {
        // This should be the same as result of .tileToLayout(md.layout)
        val rasterSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(rasterSource, md.layout)

        // Complete the reprojection
        val reprojectedSource =
          rasterSourceRDD.reproject(targetCRS, layout)._2

        println(s"reprojectedExpectedRDD.keys.collect().toList: ${reprojectedExpectedRDD.keys.collect().toList}")
        println(s"reprojectedSourceRDD.keys.collect().toList: ${reprojectedSource.keys.collect().toList}")

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSource)
      }


      it("should reproduce tileToLayout followed by reproject ONLY") {
        // This should be the same as .tileToLayout(md.layout).reproject(crs, layout)
        val reprojectedSourceRDD: MultibandTileLayerRDD[SpatialKey] =
          RasterSourceRDD(
            rasterSource
              .reproject(
                targetCRS,
                options = Reproject.Options.DEFAULT.copy(
                  //targetCellSize = Some(CellSize(19.2, 19.2))
                  targetCellSize = Some(layout.cellSize)
                )
              ),
            layout
          )

        println(s"reprojectedExpectedRDD.keys.collect().toList: ${reprojectedExpectedRDD.keys.collect().toList}")
        println(s"reprojectedSourceRDD.keys.collect().toList: ${reprojectedSourceRDD.keys.collect().toList}")

        assertRDDLayersEqual(reprojectedExpectedRDD, reprojectedSourceRDD)
      }
    }
  }
}
