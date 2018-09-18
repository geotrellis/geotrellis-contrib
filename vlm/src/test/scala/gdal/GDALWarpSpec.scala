package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
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
    val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"

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

          gdal.ApplyGeoTransform(geoTransform, bound.colMin, bound.rowMax, colMin, rowMin)
          gdal.ApplyGeoTransform(geoTransform, bound.colMax, bound.rowMin, colMax, rowMax)

          val targetExtent = Extent(colMin.head, rowMin.head, colMax.head, rowMax.head)
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
      val reprojectedImage = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled-3857.tif"

      val gdalSource = GDALRasterSource(reprojectedImage)
      val warpSource = rasterSource.withCRS(WebMercator)

      val expectedTile = gdalSource.read().next.tile.band(0)
      val actualTile = warpSource.read().next.tile.band(0)

      assertEqual(expectedTile, actualTile)
    }
  }
}
