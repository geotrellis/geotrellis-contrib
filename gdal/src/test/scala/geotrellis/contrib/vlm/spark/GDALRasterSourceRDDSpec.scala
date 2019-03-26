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

package geotrellis.contrib.vlm.spark

import java.io.File
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import cats.implicits._
import geotrellis.contrib.vlm.gdal._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.{BetterRasterMatchers, LayoutTileSource, RasterSource, ReadingSource}
import geotrellis.gdal._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import org.apache.spark.rdd.RDD
import org.scalatest.Inspectors._
import org.scalatest._
import spire.syntax.cfor._

import scala.concurrent.ExecutionContext

class GDALRasterSourceRDDSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with BeforeAndAfterAll {
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  def filePathByIndex(i: Int): String = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled-$i.tif"
  val uri = s"file://$filePath"
  val rasterSource = GeoTiffRasterSource(uri)
  val targetCRS = CRS.fromEpsgCode(3857)
  val scheme = ZoomedLayoutScheme(targetCRS)
  val layout = scheme.levelForZoom(13).layout

  val reprojectedSource = rasterSource.reprojectToGrid(targetCRS, layout)

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

      def parellSpec(n: Int = 1000)(implicit cs: ContextShift[IO]): List[RasterSource] = {
        println(java.lang.Thread.activeCount())

        /** Functions to trigger Datasets computation */
        def ltsWithDatasetsTriggered(lts: LayoutTileSource): LayoutTileSource = { rsWithDatasetsTriggered(lts.source); lts }
        def rsWithDatasetsTriggered(rs: RasterSource): RasterSource = {
          val brs = rs.asInstanceOf[GDALBaseRasterSource]
          brs.dataset.rasterExtent
          brs.baseDataset.rasterExtent
          rs
        }

        /** Do smth usual with the original RasterSource to force VRTs allocation */
        def reprojRS(i: Int): LayoutTileSource =
          ltsWithDatasetsTriggered(
            rsWithDatasetsTriggered(
              rsWithDatasetsTriggered(GDALRasterSource(filePathByIndex(i)))
                .reprojectToGrid(targetCRS, layout)
            ).tileToLayout(layout)
          )

        /** Simulate possible RF backsplash calls */
        def dirtyCalls(rs: RasterSource): RasterSource = {
          val ds = rs.asInstanceOf[GDALBaseRasterSource].dataset
          ds.rasterExtent
          ds.crs
          ds.cellSize
          ds.extent
          rs
        }

        val res = (1 to n).toList.flatMap { _ =>
          (0 to 4).flatMap { i =>
            List(IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).toIterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            }, IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).toIterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            }, IO {
              // println(Thread.currentThread().getName())
              // Thread.sleep((Math.random() * 100).toLong)
              val lts = reprojRS(i)
              lts.readAll(lts.keys.take(10).toIterator)
              reprojRS(i).source.resolutions

              dirtyCalls(reprojRS(i).source)
            })
          }
        }.parSequence.unsafeRunSync

        println(java.lang.Thread.activeCount())

        res
      }

      it(s"should not fail on parallelization with a fork join pool") {
        val i = 1000
        implicit val cs = IO.contextShift(ExecutionContext.global)

        val res = parellSpec(i)
      }

      it(s"should not fail on parallelization with a fixed thread pool") {
        val i = 1000
        val n = 200
        val pool = Executors.newFixedThreadPool(n)
        val ec = ExecutionContext.fromExecutor(pool)
        implicit val cs = IO.contextShift(ec)

        val res = parellSpec(i)
      }
    }
  }
}
