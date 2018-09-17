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
    //val uri = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
    val uri = "/tmp/test-file.tif"

    val rasterSource = GDALRasterSource(uri)
    val sourceDataset = GDAL.open(uri)

    val sourceTiff = GeoTiffReader.readMultiband(uri)

    val reprojectedRasterExtent = {
      val re = ReprojectRasterExtent(rasterSource.rasterExtent, Transform(rasterSource.crs, LatLng))
      // stretch target raster extent slightly to avoid default case in ReprojectRasterExtent
      RasterExtent(re.extent, CellSize(re.cellheight * 1.1, re.cellwidth * 1.1))
    }

    val params =
      new java.util.Vector(
        java.util.Arrays.asList(
          "-s_srs",
          rasterSource.crs.toProj4String,
          "-t_srs",
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

      val sr = new SpatialReference()
      sr.ImportFromProj4(LatLng.toProj4String)

      val vrt =
        gdal.AutoCreateWarpedVRT(
          sourceDataset,
          new SpatialReference(sourceDataset.GetProjection).ExportToWkt,
          sr.ExportToWkt,
          GDAL.deriveGDALResampleMethod(method)
        )

      val testBounds = GridBounds(0, 0, reprojectedRasterExtent.cols, reprojectedRasterExtent.rows).split(64,64).toSeq

      val updatedParams = {
        val update = params
        update.add("-r")
        update.add(s"${GDAL.deriveResampleMethodString(method)}")

        val result = update

        update.removeElement("-r")
        update.removeElement(s"${GDAL.deriveResampleMethodString(method)}")

        result
      }

      val options = new WarpOptions(updatedParams)
      val dataset = gdal.Warp("/vsimem/gdal-warp-test", Array(sourceDataset), options)

      //val geoTransform: Array[Double] = dataset.GetGeoTransform
      val geoTransform: Array[Double] = vrt.GetGeoTransform

      /*
      val cols = dataset.GetRasterXSize.toInt
      val rows = dataset.GetRasterYSize.toInt

      val extent = {
        val xmin: Double = geoTransform(0)
        val ymin: Double = geoTransform(3) + geoTransform(5) * rows
        val xmax: Double = geoTransform(0) + geoTransform(1) * cols
        val ymax: Double = geoTransform(3)

        Extent(xmin, ymin, xmax, ymax)
      }

      val rasterExtent = RasterExtent(extent, cols, rows)

      val testBounds = GridBounds(0, 0, cols, rows).split(64,64).toSeq
      */

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

          /*
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
          */

          dataset.delete

          //val actual = warpRasterSource.read(List(testRasterExtent)).next
          val actual = {
            val gb = testRasterExtent.gridBounds
            val arr = Array.ofDim[Float](testRasterExtent.cols * testRasterExtent.rows)

            vrt.ReadRaster(
              gb.colMin,
              gb.rowMin,
              gb.width,
              gb.height,
              gb.width,
              gb.height,
              gdalconstConstants.GDT_Float32,
              arr,
              Array(1)
            )

            val tile = FloatArrayTile(arr, gb.width, gb.height, -9999.0.toFloat)

            Raster(MultibandTile(tile), testRasterExtent.extent)
          }


          val expectedTile = expected.tile.band(0)
          //val actualTile = actual.tile.band(0).reproject(actual.extent, sourceTiff.crs, LatLng).tile
          val actualTile = actual.tile.band(0)

          actual.extent.covers(expected.extent) should be (true)
          actual.rasterExtent.extent.xmin should be (expected.rasterExtent.extent.xmin +- 0.00001)
          actual.rasterExtent.extent.ymax should be (expected.rasterExtent.extent.ymax +- 0.00001)
          actual.rasterExtent.cellwidth should be (expected.rasterExtent.cellwidth +- 0.00001)
          actual.rasterExtent.cellheight should be (expected.rasterExtent.cellheight +- 0.00001)

          println(s"\nThis is the expectedTile:")
          println(NumericEncoder.encodeIntegrals(expectedTile))
          println("\nThis is the actualTile:")
          println(s"${NumericEncoder.encodeIntegrals(actualTile)}\n")

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
