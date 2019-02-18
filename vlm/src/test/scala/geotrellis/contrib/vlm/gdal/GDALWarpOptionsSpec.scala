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

import geotrellis.contrib.vlm._
import geotrellis.gdal._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{CellSize, GridExtent}
import geotrellis.raster.io.geotiff.AutoHigherResolution
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.reproject.Reproject.{Options => ReprojectOptions}
import geotrellis.vector.Extent
import geotrellis.raster.testkit._

import org.gdal.gdal.Dataset
import cats.implicits._

import org.scalatest._

class GDALWarpOptionsSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  import GDALWarpOptionsSpec._

  val filePath = Resource.path("img/aspect-tiled.tif")
  def filePathByIndex(i: Int): String = Resource.path(s"img/aspect-tiled-$i.tif")

  val reprojectOptions: GDALWarpOptions =
    generateWarpOptions(
      sourceCRS = Some(CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs ")),
      targetCRS = Some(WebMercator),
      cellSize = Some(CellSize(10, 10))
    )

  val resampleOptions: GDALWarpOptions =
    generateWarpOptions(
      et        = None,
      cellSize  = Some(CellSize(22, 22)),
      sourceCRS = None,
      targetCRS = None
    )

  def dsreproject(dataset: Dataset): Dataset =
    GDAL.warp("", dataset, reprojectOptions, None)
  def dsresample(dataset: Dataset, uri: Option[String]): Dataset =
    GDAL.warp("", dataset, resampleOptions, uri.map(str => str -> List(reprojectOptions)))

  def dsreprojectOpt(dataset: Dataset): Dataset = {
    val opts =
      GDALWarpOptions(Some("MEM"))
        .reproject(
          rasterExtent = GridExtent(Extent(630000.0, 215000.0, 645000.0, 228500.0), 10, 10).toRasterExtent,
          CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs "),
          WebMercator,
          ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(10, 10).some)
        )

    GDAL.warp("", dataset, opts, None)
  }
  def dsresampleOpt(dataset: Dataset): Dataset = {
    val opts =
      GDALWarpOptions(Some("MEM"))
        .reproject(
          rasterExtent = GridExtent(Extent(630000.0, 215000.0, 645000.0, 228500.0), 10, 10).toRasterExtent,
          CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs "),
          WebMercator,
          ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(10, 10).some)
        )
        .resample(
          GridExtent(Extent(-8769160.0, 4257700.0, -8750630.0, 4274460.0), 10, 10).toRasterExtent,
          TargetRegion(GridExtent(Extent(-8769160.0, 4257700.0, -8750630.0, 4274460.0), 22, 22).toRasterExtent)
        )

    GDAL.warp("", dataset, opts, None)
  }

  describe("GDALWarp transformations") {
    it("optimized transformation should behave in a same way as a list of warp applications") {
      val base = GDAL.open(filePath)

      val originalReproject = dsreproject(base)
      val originalResample = dsresample(originalReproject, Some(filePath))

      val optimizedReproject = dsreprojectOpt(base)
      val optimizedResample = dsresampleOpt(base)

      originalReproject.rasterExtent shouldBe optimizedReproject.rasterExtent
      originalResample.rasterExtent shouldBe optimizedResample.rasterExtent
    }

    it("raster sources optimized transformations should behave in a same way as a single warp application") {
      val base = GDAL.open(filePath)
      val optimizedRawResample = dsresampleOpt(base)
      val originalRawResample = dsresample(dsreproject(base), Some(filePath))

      val rs =
        GDALRasterSource(filePath)
          .reproject(
            targetCRS        = WebMercator,
            reprojectOptions = ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(10, 10).some),
            strategy         = AutoHigherResolution
          )
          .resampleToRegion(
            region = GridExtent(Extent(-8769160.0, 4257700.0, -8750630.0, 4274460.0), 22, 22).toRasterExtent
          )

      optimizedRawResample.rasterExtent shouldBe rs.rasterExtent
      originalRawResample.rasterExtent shouldBe rs.rasterExtent
    }
  }
}

object GDALWarpOptionsSpec {
  def generateWarpOptions(
    et: Option[Double] = Some(0.125),
    cellSize: Option[CellSize] = Some(CellSize(19.1, 19.1)),
    sourceCRS: Option[CRS] = Some(CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs ")),
    targetCRS: Option[CRS] = Some(WebMercator)
  ): GDALWarpOptions = {
    GDALWarpOptions(
      Some("VRT"),
      Some(NearestNeighbor),
      et,
      cellSize,
      true,
      None,
      sourceCRS,
      targetCRS,
      None,
      None,
      List("-9999.0"),
      Nil,
      Some(AutoHigherResolution),
      Nil, false, None, false, false, false, None, Nil, None, None, false, false,
      false, None, false, false, Nil, None, None, None, None, None, false, false, false, None, false, Nil, Nil, Nil, None
    )
  }
}
