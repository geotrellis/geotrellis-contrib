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
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.AutoHigherResolution
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject.{Options => ReprojectOptions}
import geotrellis.vector.Extent
import geotrellis.raster.testkit._

import org.gdal.gdal._

import com.azavea.gdal.GDALWarp

import cats.implicits._

import scala.collection.JavaConverters._

import org.scalatest._


class GDALWarpOptionsSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  import GDALWarpOptionsSpec._

  GDALWarp.init(1<<8, 1<<2)

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

  def rasterSourceFromUriOptions(uri: String, options: GDALWarpOptions) = {
    GDALRasterSource(uri, options)
  }

  def dsreprojectOpt(uri: String) = {
    val opts =
      GDALWarpOptions()
        .reproject(
          rasterExtent = GridExtent(Extent(630000.0, 215000.0, 645000.0, 228500.0), 10, 10).toRasterExtent,
          CRS.fromString("+proj=lcc +lat_1=36.16666666666666 +lat_2=34.33333333333334 +lat_0=33.75 +lon_0=-79 +x_0=609601.22 +y_0=0 +datum=NAD83 +units=m +no_defs "),
          WebMercator,
          ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(10, 10).some)
        )
    rasterSourceFromUriOptions(uri, opts)
  }

  def dsresampleOpt(uri: String) = {
    val opts =
      GDALWarpOptions()
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
    rasterSourceFromUriOptions(uri, opts)
  }

  describe("GDALWarp transformations") {
    def datasetToRasterExtent(ds: Dataset): RasterExtent = {
      val transform = ds.GetGeoTransform
      val width = ds.GetRasterXSize
      val height = ds.GetRasterYSize
      val x1 = transform(0)
      val y1 = transform(3)
      val x2 = x1 + transform(1) * width
      val y2 = y1 + transform(5) * height
      val e = Extent(
        math.min(x1, x2),
        math.min(y1, y2),
        math.max(x1, x2),
        math.max(y1, y2))

      RasterExtent(e,
        math.abs(transform(1)), math.abs(transform(5)),
        width, height)
    }

    it("optimized transformation should behave in a same way as a list of warp applications") {
      val base = filePath

      val optimizedReproject = dsreprojectOpt(base)
      val optimizedResample = dsresampleOpt(base)

      val (originalReproject, originalResample) = {
        val reprojectWarpAppOptions = new WarpOptions(new java.util.Vector(reprojectOptions.toWarpOptionsList.asJava)) // This is probably a leak
        val resampleWarpAppOptions = new WarpOptions(new java.util.Vector(resampleOptions.toWarpOptionsList.asJava)) // So is this
        val underlying = org.gdal.gdal.gdal.Open(filePath, org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly)
        val originalReproject = org.gdal.gdal.gdal.Warp("/dev/null", Array(underlying), reprojectWarpAppOptions)
        val originalResample = org.gdal.gdal.gdal.Warp("/dev/null", Array(originalReproject), resampleWarpAppOptions)
        (originalReproject, originalResample)
      }

      datasetToRasterExtent(originalReproject) shouldBe optimizedReproject.rasterExtent
      datasetToRasterExtent(originalResample) shouldBe optimizedResample.rasterExtent
    }

    it("raster sources optimized transformations should behave in a same way as a single warp application") {
      val base = filePath

      val optimizedRawResample = dsresampleOpt(base)

      val originalRawResample = {
        val reprojectWarpAppOptions = new WarpOptions(new java.util.Vector(reprojectOptions.toWarpOptionsList.asJava)) // ditto
        val resampleWarpAppOptions = new WarpOptions(new java.util.Vector(resampleOptions.toWarpOptionsList.asJava)) // ditto
        val underlying = org.gdal.gdal.gdal.Open(filePath, org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly)
        val reprojected = org.gdal.gdal.gdal.Warp("/dev/null", Array(underlying), reprojectWarpAppOptions)
        org.gdal.gdal.gdal.Warp("/dev/null", Array(reprojected), resampleWarpAppOptions)
      }

      val rs =
        GDALRasterSource(filePath)
          .reproject(
            targetCRS        = WebMercator,
            reprojectOptions = ReprojectOptions.DEFAULT.copy(targetCellSize = CellSize(10, 10).some),
            strategy         = AutoHigherResolution
        )
          .resampleToRegion(
            region = RasterExtent(Extent(-8769160.0, 4257700.0, -8750630.0, 4274460.0), CellSize(22, 22))
        )

      optimizedRawResample.rasterExtent shouldBe rs.rasterExtent
      datasetToRasterExtent(originalRawResample) shouldBe rs.rasterExtent
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
