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
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._
import geotrellis.gdal._
import geotrellis.gdal.config._

import org.apache.spark.rdd.RDD

import cats.implicits._
import cats.effect.{ContextShift, IO}
import spire.syntax.cfor._
import org.scalatest._
import Inspectors._

import java.io.File
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

class InvestigationSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with BeforeAndAfterAll {
  it("should compute the histogram for the layer") {
    val basePath = "s3://azavea-datahub/raw/nlcd/nlcd_2011_landcover_full_raster_30m/"
    val baseName = "nlcd_2011_landcover_2011_01"
    val base = s"$basePath$baseName"

    val targetCRS = CRS.fromEpsgCode(3857)
    val method = NearestNeighbor
    val scheme = ZoomedLayoutScheme(targetCRS)
    val layout = scheme.levelForZoom(13).layout

    val paths: Seq[String] =
      Seq(
        s"${base}_02.tif",
        s"${base}_03.tif",
        s"${base}_04.tif",
        s"${base}_05.tif",
        s"${base}_06.tif",
        s"${base}_07.tif"
      )

    val rdd = sc.parallelize(paths, paths.size).cache()
    val sourcesRDD = rdd.map { GDALRasterSource(_)}
    val reprojected = sourcesRDD.map { _.reproject(targetCRS, method) }
    val contextRDD = RasterSourceRDD.tiledLayerRDD(reprojected, layout, method)
    contextRDD.histogram
  }
}
