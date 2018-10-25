package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent

import org.apache.spark.rdd._
import org.scalatest._

  class RasterSummarySpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  describe("Should collect GeoTiffRasterSource RasterSummary correct") {
    it("should collect summary for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => new GeoTiffRasterSource(uri): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD(sourceRDD)
      val rasterSource = new GeoTiffRasterSource(inputPath)

      rasterSource.crs shouldBe metadata.crs
      rasterSource.extent shouldBe metadata.extent
      rasterSource.cellSize shouldBe metadata.cellSize
      rasterSource.cellType shouldBe metadata.cellType
      rasterSource.size shouldBe metadata.cells
      files.length shouldBe metadata.count
    }

    it("should collect summary for a tiled to layout source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil
      val targetCRS = WebMercator
      val method = Bilinear
      val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => new GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD(sourceRDD)
      val layout = metadata.levelFor(layoutScheme).layout
      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val metadataCollected = RasterSummary.fromRDD(tiledRDD.map(_.source))
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

  describe("Should collect GDALRasterSource RasterSummary correct") {
    it("should collect summary for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => GDALRasterSource(uri): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD(sourceRDD)
      val rasterSource = GDALRasterSource(inputPath)

      rasterSource.crs shouldBe metadata.crs
      rasterSource.extent shouldBe metadata.extent
      rasterSource.cellSize shouldBe metadata.cellSize
      rasterSource.cellType shouldBe metadata.cellType
      rasterSource.size shouldBe metadata.cells
      files.length shouldBe metadata.count
    }

    // TODO: the problem is in a GDAL -tap parameter usage
    // should be fixed
    ignore("should collect summary for a tiled to layout source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil
      val targetCRS = WebMercator
      val method = Bilinear
      val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => GDALRasterSource(uri).reproject(targetCRS, method): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD(sourceRDD)
      val layout = metadata.levelFor(layoutScheme).layout
      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val metadataCollected = RasterSummary.fromRDD(tiledRDD.map(_.source))
      val metadataResampled = metadata.resample(TargetGrid(layout))

      metadataCollected.crs shouldBe metadataResampled.crs
      metadataCollected.cellType shouldBe metadataResampled.cellType

      val CellSize(widthCollected, heightCollected) = metadataCollected.cellSize
      val CellSize(widthResampled, heightResampled) = metadataResampled.cellSize

      // the only weird place where cellSize is a bit different
      widthCollected shouldBe (widthResampled +- 1e-7)
      heightCollected shouldBe (heightResampled +- 1e-7)

      val Extent(xminc, yminc, xmaxc, ymaxc) = metadataCollected.extent
      val Extent(xminr, yminr, xmaxr, ymaxr) = metadataResampled.extent

      // extent probably can be calculated a bit different via GeoTrellis API
      xminc shouldBe xminr +- 1e-5
      yminc shouldBe yminr +- 1e-5
      xmaxc shouldBe xmaxr +- 1e-5
      ymaxc shouldBe ymaxr +- 1e-5

      metadataCollected.cells shouldBe metadataResampled.cells
      metadataCollected.count shouldBe metadataResampled.count
    }
  }
}