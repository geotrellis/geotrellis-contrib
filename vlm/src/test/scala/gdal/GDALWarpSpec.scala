package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.testkit._
import geotrellis.proj4._
import geotrellis.spark.testkit._
import geotrellis.vector.Extent

import org.scalatest._

import java.io.File

import org.gdal.gdal._
import org.gdal.gdalconst.gdalconstConstants


class GDALWarpSpec extends FunSpec with TestEnvironment with RasterMatchers {
  describe("Reprojecting a GDALRasterSource") {
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"

    val rasterSource = GDALRasterSource(uri)
    val sourceDataset = GDAL.open(uri)

    val params =
      new java.util.Vector(
        java.util.Arrays.asList(
          "-a_srs",
          LatLng.toProj4String
        )
      )

    val noDataValue: Option[Double] = {
      val arr = Array.ofDim[java.lang.Double](1)
      sourceDataset.GetRasterBand(1).GetNoDataValue(arr)

      arr.head match {
        case null => None
        case value => Some(value.doubleValue)
      }
    }

    val ct = noDataValue match {
      case Some(nd) => FloatUserDefinedNoDataCellType(nd.toFloat)
      case _ => FloatConstantNoDataCellType
    }

    def testReprojection(method: ResampleMethod) = {
      val warpRasterSource = rasterSource.withCRS(LatLng, method)

      val updatedParams = {
        val update = params
        update.add("-r")
        update.add(s"${GDAL.deriveResampleMethodString(method)}")

        val result = update

        update.removeElement("-r")
        update.removeElement(s"${GDAL.deriveResampleMethodString(method)}")
        result
      }

      val options = new TranslateOptions(updatedParams)
      val dataset = gdal.Translate("/vsimem/gdal-warp-test", sourceDataset, options)

      val cols = dataset.GetRasterXSize.toInt
      val rows = dataset.GetRasterYSize.toInt

      val testBounds =
        GridBounds(0, 0, cols, rows).split(64,64).toSeq

      val extent = {
        val geoTransform: Array[Double] = dataset.GetGeoTransform

        val xmin: Double = geoTransform(0)
        val ymin: Double = geoTransform(3) + geoTransform(5) * rows
        val xmax: Double = geoTransform(0) + geoTransform(1) * cols
        val ymax: Double = geoTransform(3)

        Extent(xmin, ymin, xmax, ymax)
      }

      val rasterExtent = RasterExtent(extent, cols, rows)

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val targetExtent = rasterExtent.extentFor(bound)
          val testRasterExtent = RasterExtent(targetExtent, cols = bound.width, rows = bound.height)

          val expected: Raster[MultibandTile] = {
            val arr = Array.ofDim[Float](bound.width * bound.height)

            dataset
              .ReadRaster(
                bound.colMin,
                bound.rowMin,
                bound.width,
                bound.height,
                bound.width,
                bound.height,
                gdalconstConstants.GDT_Float32,
                arr,
                Array(1)
              )

            val tile = MultibandTile(FloatArrayTile(arr, bound.width, bound.height, ct))

            Raster(tile, targetExtent)
          }

          dataset.delete

          val actual = warpRasterSource.read(List(testRasterExtent)).next

          val expectedTile = expected.tile.band(0)
          val actualTile = actual.tile.band(0)

          actual.extent.covers(expected.extent) should be (true)
          actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
          actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
          actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
          actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)
          assertEqual(actual, expected)
        }
      }
    }

    it("should reproject using NearestNeighbor") {
      testReprojection(NearestNeighbor)
    }

    it("should reproject using Bilinear") {
      testReprojection(Bilinear)
    }
  }
}
