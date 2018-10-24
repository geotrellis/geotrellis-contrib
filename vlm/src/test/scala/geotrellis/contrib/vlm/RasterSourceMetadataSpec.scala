package geotrellis.contrib.vlm

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._

import org.apache.spark.rdd._

import org.scalatest._

class RasterSourceMetadataSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  describe("Should collect RasterSource metadata correct") {
    it("should collect metadata for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => new GeoTiffRasterSource(uri): RasterSource)
          .cache()

      val metadata = RasterSourceMetadata.fromRDD(sourceRDD)
      val rasterSource = new GeoTiffRasterSource(inputPath)

      rasterSource.crs shouldBe metadata.crs
      rasterSource.extent shouldBe metadata.extent
      rasterSource.cellSize shouldBe metadata.cellSize
      rasterSource.cellType shouldBe metadata.cellType
      rasterSource.size shouldBe metadata.cells
      files.length shouldBe metadata.count
    }

    it("should collect metadata for a tiled to layout source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil
      val targetCRS = WebMercator
      val method = Bilinear
      val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => new GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource)
          .cache()

      val metadata = RasterSourceMetadata.fromRDD(sourceRDD)
      val layout = metadata.levelFor(layoutScheme).layout
      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val metadataCollected = RasterSourceMetadata.fromRDD(tiledRDD.map(_.source))
      val metadataResampled = metadata.resample(TargetGrid(layout))

      metadataCollected.crs shouldBe metadataResampled.crs
      metadataCollected.cellType shouldBe metadataResampled.cellType

      val CellSize(widthCollected, heightCollected) = metadataCollected.cellSize
      val CellSize(widthResampled, heightResampled) = metadataResampled.cellSize

      // the only weird place where cellSize is a bit different
      widthCollected shouldBe (widthResampled +- 1e-7)
      heightCollected shouldBe (heightResampled +- 1e-7)

      metadataCollected.extent shouldBe metadataResampled.extent
      metadataCollected.cells shouldBe metadataResampled.cells
      metadataCollected.count shouldBe metadataResampled.count
    }
  }
}