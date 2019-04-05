package geotrellis.contrib.vlm.gdal

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.testkit._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.Extent
import com.azavea.gdal.GDALWarp

import org.scalatest._
import java.io.File

class GDALWarpReadTileSpec extends FunSpec with RasterMatchers {
  val path = s"${new File("").getAbsolutePath}/src/test/resources/img/slope.tif"

  describe("reading a GeoTiff") {
    it("should read full raster correct") {
      val filePath = s"${new File("").getAbsolutePath}/src/test/resources/img/aspect-tiled.tif"
      val dataset = GDALWarp.get_token(filePath, Array())
      val gdalTile = dataset.readMultibandTile()
      val gtTile = GeoTiffReader.readMultiband(filePath).tile.toArrayTile

      gdalTile.cellType shouldBe gtTile.cellType
      assertEqual(gdalTile, gtTile)
    }

    it("should read a raster with bad nodata value set correct") {
      val filePath = s"${new File("").getAbsolutePath}/src/test/resources/img/badnodata.tif"
      // using a small extent to make tests work faster
      val ext = Extent(680138.59203, 4904905.667, 680189.7, 4904955.9)
      val dataset = GDALWarp.get_token(filePath, Array())
      val gdalTile = dataset.readMultibandTile(dataset.rasterExtent.gridBoundsFor(ext, clamp = false))
      val gtTile = GeoTiffReader.readMultiband(filePath, ext).tile.toArrayTile

      gdalTile.cellType shouldBe gtTile.cellType
      assertEqual(gdalTile, gtTile)
    }

    it("should match one read with GeoTools") {
      println("Reading with GDAL...")
      val dataset = GDALWarp.get_token(path, Array())
      val raster = Raster(dataset.readMultibandTile(), dataset.rasterExtent.extent)
      val gdRaster = raster.tile.band(0)
      val gdExt = raster.extent
      println("Reading with GeoTools....")
      val Raster(gtRaster, gtExt) = SinglebandGeoTiff(path).raster
      println("Done.")

      gdExt.xmin should be(gtExt.xmin +- 0.00001)
      gdExt.xmax should be(gtExt.xmax +- 0.00001)
      gdExt.ymin should be(gtExt.ymin +- 0.00001)
      gdExt.ymax should be(gtExt.ymax +- 0.00001)

      gdRaster.cols should be(gtRaster.cols)
      gdRaster.rows should be(gtRaster.rows)

      gdRaster.cellType.toString.take(7) should be(gtRaster.cellType.toString.take(7))

      println("Comparing rasters...")
      for (col <- 0 until gdRaster.cols) {
        for (row <- 0 until gdRaster.rows) {
          val actual = gdRaster.getDouble(col, row)
          val expected = gtRaster.getDouble(col, row)
          withClue(s"At ($col, $row): GDAL - $actual  GeoTools - $expected") {
            isNoData(actual) should be(isNoData(expected))
            if (isData(actual)) actual should be(expected)
          }
        }
      }
    }

    it("should do window reads") {
      val dataset = GDALWarp.get_token(path, Array())
      val gtiff = MultibandGeoTiff(path)
      val gridBounds = dataset.rasterExtent.gridBounds.split(15, 21)

      gridBounds.foreach { gb =>
        val actualTile = dataset.readMultibandTile(gb).tile
        val expectedTile = gtiff.tile.crop(gb)

        assertEqual(actualTile, expectedTile)
      }
    }

    it("should read CRS from file") {
      val dataset = GDALWarp.get_token(s"${new File("").getAbsolutePath}/src/test/resources/img/all-ones.tif", Array())
      dataset.crs.epsgCode should equal(LatLng.epsgCode)
    }
  }

  describe("reading a JPEG2000") {
    val lengthExpected = 100
    type TypeExpected = UShortCells
    val jpeg2000Path = s"${new File("").getAbsolutePath}/src/test/resources/img/jpeg2000-test-files/testJpeg2000.jp2"
    val jpegTiffPath = s"${new File("").getAbsolutePath}/src/test/resources/img/jpeg2000-test-files/jpegTiff.tif"

    val jpegDataset = GDALWarp.get_token(jpeg2000Path, Array())
    val tiffDataset = GDALWarp.get_token(jpegTiffPath, Array())

    val gridBounds: Iterator[GridBounds[Int]] =
      jpegDataset.rasterExtent.gridBounds.split(20, 15)

    it("should read a JPEG2000 from a file") {
      val raster = Raster(jpegDataset.readMultibandTile(), jpegDataset.rasterExtent.extent)
      val tile = raster.tile
      val extent = raster.rasterExtent

      extent.cols should be(lengthExpected)
      extent.rows should be(lengthExpected)
      tile.cellType shouldBe a[TypeExpected]
    }

    it("should do window reads") {
      gridBounds.foreach { gb =>
        val actualTile = jpegDataset.readMultibandTile(gb)
        val expectedTile = tiffDataset.readMultibandTile(gb)

        assertEqual(actualTile, expectedTile)
      }
    }
  }
}