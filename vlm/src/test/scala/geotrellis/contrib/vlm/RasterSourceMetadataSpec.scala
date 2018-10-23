package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster.{CellSize, RasterExtent}
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import org.apache.spark.rdd._
import org.scalatest.Inspectors._
import org.scalatest._

class RasterSourceMetadataSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  describe("Should collect RasterSource metadata correct") {
    it("should collect metadata for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      // for a given list of files
      val files = inputPath :: Nil

      // we can read in the infomration about input raster sources
      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => new GeoTiffRasterSource(uri): RasterSource)
          .cache()

      // after reading, we need to collect metadata for all input RasterSources
      val metadata = {
        val all = RasterSourceMetadata.collect(sourceRDD)
        println(s"Raster Summary: ${all.toList}")
        require(all.size == 1, "multiple CRSs detected") // what to do in this case?
        all.head // assuming we have a single one
      }

      val layout = metadata.layoutDefinition(new FloatingLayoutScheme(512, 512))


      println(s"metadata: ${metadata}")
      println(s"layout: ${layout}")
    }

    it("should collect metadata for a tiled to layout source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      // for a given list of files
      val files = inputPath :: Nil
      val targetCRS = WebMercator

      val method = Bilinear
      val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)


      // we can read in the infomration about input raster sources
      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => {
            val rs = new GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource
            println(s"rs.extent: ${rs.extent}")
            rs
          })
          .cache()

      // after reading, we need to collect metadata for all input RasterSources
      val metadata = {
        val all = RasterSourceMetadata.collect(sourceRDD)
        println(s"Raster Summary: ${all.toList}")
        require(all.size == 1, "multiple CRSs detected") // what to do in this case?
        all.head // assuming we have a single one
      }

      val ld = metadata.layoutDefinition(new FloatingLayoutScheme(512, 512))

      val LayoutLevel(globalZoom, layout) = layoutScheme.levelFor(metadata.extent, metadata.cellSize)

      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val metadataCollected = {
        val all = RasterSourceMetadata.collect(tiledRDD.map(_.source))
        require(all.size == 1, "multiple CRSs detected") // what to do in this case?
        all.head // assuming we have a single one
      }

      val metadataResampled = metadata.resample(TargetGrid(layout))

      metadataCollected.crs shouldBe metadataResampled.crs
      metadataCollected.cellType shouldBe metadataResampled.cellType

      val CellSize(widthCollected, heightCollected) = metadataCollected.cellSize
      val CellSize(widthResampled, heightResampled) = metadataResampled.cellSize

      widthCollected shouldBe (widthResampled +- 1e-7)
      heightCollected shouldBe (heightResampled +- 1e-7)

      metadataCollected.extent shouldBe metadataResampled.extent
      metadataCollected.cells shouldBe metadataResampled.cells
      metadataCollected.count shouldBe metadataResampled.count
    }
  }
}