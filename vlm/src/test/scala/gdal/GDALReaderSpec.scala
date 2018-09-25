package geotrellis.contrib.vlm.gdal

import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import geotrellis.raster.testkit._
import geotrellis.spark.testkit._
import org.scalatest._

import java.io.File

class GDALReaderSpec
  extends FunSpec
    with TestEnvironment
    with RasterMatchers {

  describe("GDALReaderSpec") {
    it("should read full raster correct") {
      val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
      val gdalTile = GDALReader(filePath).read()
      val gtTile   = GeoTiffReader.readMultiband(filePath).tile

      assertEqual(gdalTile, gtTile)
    }
  }
}
