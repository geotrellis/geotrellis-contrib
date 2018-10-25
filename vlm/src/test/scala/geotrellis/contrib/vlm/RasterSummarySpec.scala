package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
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

      val summary = RasterSummary.fromRDD(sourceRDD)
      val layoutLevel @ LayoutLevel(zoom, layout) = summary.levelFor(layoutScheme)
      val tiledLayoutSource = sourceRDD.map(_.tileToLayout(layout, method))

      val summaryCollected = RasterSummary.fromRDD(tiledLayoutSource.map(_.source))
      val summaryResampled = summary.resample(TargetGrid(layout))

      val metadata = summary.toTileLayerMetadata(layoutLevel)
      val metadataResampled = summaryResampled.toTileLayerMetadata(GlobalLayout(256, zoom, 0.1))

      metadata shouldBe metadataResampled

      summaryCollected.crs shouldBe summaryResampled.crs
      summaryCollected.cellType shouldBe summaryResampled.cellType

      val CellSize(widthCollected, heightCollected) = summaryCollected.cellSize
      val CellSize(widthResampled, heightResampled) = summaryResampled.cellSize

      // the only weird place where cellSize is a bit different
      widthCollected shouldBe (widthResampled +- 1e-7)
      heightCollected shouldBe (heightResampled +- 1e-7)

      summaryCollected.extent shouldBe summaryResampled.extent
      summaryCollected.cells shouldBe summaryResampled.cells
      summaryCollected.count shouldBe summaryResampled.count
    }
  }

  it("should create ContextRDD from RDD of RasterSources") {
    val inputPath = Resource.path("img/aspect-tiled.tif")
    val files = inputPath :: Nil
    val targetCRS = WebMercator
    val method = Bilinear
    val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

    // read sources
    val sourceRDD: RDD[RasterSource] =
      sc.parallelize(files, files.length)
        .map(uri => new GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource)
        .cache()

    // collect raster summary
    val summary = RasterSummary.fromRDD(sourceRDD)
    val layoutLevel @ LayoutLevel(_, layout) = summary.levelFor(layoutScheme)
    val tiledLayoutSource = sourceRDD.map(_.tileToLayout(layout, method))

    // Create RDD of references, references contain information how to read rasters
    val rasterRefRdd: RDD[(SpatialKey, RasterRef)] = tiledLayoutSource.flatMap(_.toRasterRefs)
    val tileRDD: RDD[(SpatialKey, MultibandTile)] =
      rasterRefRdd // group by keys and distribute raster references using SpatialPartitioner
        .groupByKey(SpatialPartitioner(summary.estimatePartitionsNumber))
        .mapValues { iter => MultibandTile(iter.flatMap(_.raster.toSeq.flatMap(_.tile.bands))) } // read rasters

    val (metadata, zoom) = summary.toTileLayerMetadata(layoutLevel)
    val contextRDD: MultibandTileLayerRDD[SpatialKey] = ContextRDD(tileRDD, metadata)

    contextRDD.count() shouldBe rasterRefRdd.count()
    contextRDD.count() shouldBe 72

    contextRDD.stitch.tile.band(0).renderPng().write("/tmp/raster-source-contextrdd.png")
  }

  describe("Should collect GDALRasterSource RasterSummary correct") {
    it("should collect summary for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => GDALRasterSource(uri): RasterSource)
          .cache()

      val summary = RasterSummary.fromRDD(sourceRDD)
      val rasterSource = GDALRasterSource(inputPath)

      rasterSource.crs shouldBe summary.crs
      rasterSource.extent shouldBe summary.extent
      rasterSource.cellSize shouldBe summary.cellSize
      rasterSource.cellType shouldBe summary.cellType
      rasterSource.size shouldBe summary.cells
      files.length shouldBe summary.count
    }

    // TODO: the problem is in a GDAL -tap parameter usage
    // should be fixed
    // actually GDAL version of seqential computations work slower
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

      val summary = RasterSummary.fromRDD(sourceRDD)
      val layout = summary.levelFor(layoutScheme).layout
      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val summaryCollected = RasterSummary.fromRDD(tiledRDD.map(_.source))
      val summaryResampled = summary.resample(TargetGrid(layout))

      summaryCollected.crs shouldBe summaryResampled.crs
      summaryCollected.cellType shouldBe summaryResampled.cellType

      val CellSize(widthCollected, heightCollected) = summaryCollected.cellSize
      val CellSize(widthResampled, heightResampled) = summaryResampled.cellSize

      // the only weird place where cellSize is a bit different
      widthCollected shouldBe (widthResampled +- 1e-7)
      heightCollected shouldBe (heightResampled +- 1e-7)

      val Extent(xminc, yminc, xmaxc, ymaxc) = summaryCollected.extent
      val Extent(xminr, yminr, xmaxr, ymaxr) = summaryResampled.extent

      // extent probably can be calculated a bit different via GeoTrellis API
      xminc shouldBe xminr +- 1e-5
      yminc shouldBe yminr +- 1e-5
      xmaxc shouldBe xmaxr +- 1e-5
      ymaxc shouldBe ymaxr +- 1e-5

      summaryCollected.cells shouldBe summaryResampled.cells
      summaryCollected.count shouldBe summaryResampled.count
    }
  }

  // TODO: fix JNI management here
  // this test works as expected, though crashes with a core dump exception
  // works slower than a GeoTiff version, there is an interesting discussion here: https://github.com/OpenDroneMap/OpenDroneMap/issues/778
  // also mb we should pass a resampled RasterExtent like we did in a GeoTiff case
  ignore("should create ContextRDD from RDD of RasterSources") {
    val inputPath = Resource.path("img/aspect-tiled.tif")
    val files = inputPath :: Nil
    val targetCRS = WebMercator
    val method = Bilinear
    val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

    // read sources
    val sourceRDD: RDD[RasterSource] =
      sc.parallelize(files, files.length)
        .map(uri => GDALRasterSource(uri).reproject(targetCRS, method): RasterSource)
        .cache()

    // collect raster summary
    val summary = RasterSummary.fromRDD(sourceRDD)
    val layoutLevel @ LayoutLevel(_, layout) = summary.levelFor(layoutScheme)
    val tiledLayoutSource = sourceRDD.map(_.tileToLayout(layout, method))

    // Create RDD of references, references contain information how to read rasters
    val rasterRefRdd: RDD[(SpatialKey, RasterRef)] = tiledLayoutSource.flatMap(_.toRasterRefs)
    val tileRDD: RDD[(SpatialKey, MultibandTile)] =
      rasterRefRdd // group by keys and distribute raster references using SpatialPartitioner
        .groupByKey(SpatialPartitioner(summary.estimatePartitionsNumber))
        .mapValues { iter => MultibandTile(iter.flatMap(_.raster.toSeq.flatMap(_.tile.bands))) } // read rasters

    val (metadata, zoom) = summary.toTileLayerMetadata(layoutLevel)
    val contextRDD: MultibandTileLayerRDD[SpatialKey] = ContextRDD(tileRDD, metadata)

    contextRDD.count() shouldBe rasterRefRdd.count()
    contextRDD.count() shouldBe 72

    contextRDD.stitch.tile.band(0).renderPng().write("/tmp/raster-source-contextrdd-gdal.png")
  }
}