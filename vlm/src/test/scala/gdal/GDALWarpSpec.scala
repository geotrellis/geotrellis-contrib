package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.writer._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.prototype._
import geotrellis.raster.render.ascii._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.raster.testkit._
import geotrellis.proj4._
import geotrellis.spark.testkit._
import geotrellis.vector.Extent

import org.scalatest._

import java.io.File

import org.gdal.gdal._
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference


class GDALWarpSpec extends FunSpec with TestEnvironment with RasterMatchers {
  describe("Reprojecting a GDALRasterSource") {
    //val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val uri = "/tmp/test.tif"

    val rasterSource = GDALRasterSource(uri)

    val sourceTiff = GeoTiffReader.readMultiband(uri)
    val sourceDataset = GDAL.open(uri)

    val reprojectedRasterExtent = {
      val re = ReprojectRasterExtent(rasterSource.rasterExtent, Transform(rasterSource.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    def testReprojection(method: ResampleMethod) = {
      val warpRasterSource = rasterSource.withCRS(LatLng, method)

      val sr = new SpatialReference()
      sr.ImportFromProj4(LatLng.toProj4String)

      val vrt =
        gdal.AutoCreateWarpedVRT(
          sourceDataset,
          sourceDataset.GetProjection,
          sr.ExportToWkt,
          GDAL.deriveGDALResampleMethod(method)
        )

      val testBounds = GridBounds(0, 0, reprojectedRasterExtent.cols, reprojectedRasterExtent.rows).split(64,64).toSeq

      val geoTransform: Array[Double] = vrt.GetGeoTransform
      vrt.delete

      for (bound <- testBounds) yield {
        withClue(s"Read window ${bound}: ") {
          val (colMin, rowMin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
          val (colMax, rowMax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

          //gdal.ApplyGeoTransform(geoTransform, bound.colMin, bound.rowMax, colMin, rowMin)
          //gdal.ApplyGeoTransform(geoTransform, bound.colMax, bound.rowMin, colMax, rowMax)

          val targetExtent = reprojectedRasterExtent.extentFor(bound)

          //val targetExtent = Extent(colMin.head, rowMin.head, colMax.head, rowMax.head)
          val testRasterExtent = RasterExtent(targetExtent, cols = bound.width, rows = bound.height)

          val expected: Raster[MultibandTile] = {
            val rr = implicitly[RasterRegionReproject[MultibandTile]]
            rr.regionReproject(
              sourceTiff.raster,
              sourceTiff.crs,
              LatLng,
              testRasterExtent,
              testRasterExtent.extent.toPolygon,
              method
            )
          }

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

    it("should produce the same thing as GDALRasterSource") {
      val reprojectedImage = "/tmp/slope_wsg84-nearestneighbor-er0.125.tif"
      val uri2 = "/tmp/slope_webmercator.tif"

      //val reprojectedImage = "/tmp/test-file-gt-3857.tif"
      //val uri2 = "/tmp/test-file.tif"

      val ds = GDAL.open(uri2)

      val gdalSource = GDALRasterSource(reprojectedImage)
      val warpSource = GDALRasterSource(uri2).withCRS(LatLng)

      /*
      val sr = new SpatialReference()
      sr.ImportFromProj4(WebMercator.toProj4String)

      val vrt =
        gdal.AutoCreateWarpedVRT(
          ds,
          ds.GetProjection,
          sr.ExportToWkt,
          GDAL.deriveGDALResampleMethod(NearestNeighbor)
        )

      val reader = GDALReader(vrt)

      val tile = {
        val arr = Array.ofDim[Float](warpSource.cols * warpSource.rows)

        vrt.ReadRaster(
          warpSource.rasterExtent.gridBounds.colMin,
          warpSource.rasterExtent.gridBounds.rowMin,
          warpSource.cols,
          warpSource.rows,
          warpSource.cols,
          warpSource.rows,
          gdalconstConstants.GDT_Float32,
          arr,
          Array(1)
        )

        FloatArrayTile(arr, warpSource.cols, warpSource.rows, FloatUserDefinedNoDataCellType(-9999.0.toFloat))
      }
      */

      val expectedTile = gdalSource.read().next.tile.band(0)
      val actualTile = warpSource.read().next.tile.band(0)

      assertEqual(expectedTile, actualTile)
    }
  }
}
