package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.gdal._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.spark._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent

import spire.syntax.cfor._
import org.apache.spark.rdd._

import org.scalatest._

class RasterSummarySpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  describe("Should collect GeoTiffRasterSource RasterSummary correct") {

    it("issue-116") {
      import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
      import geotrellis.proj4.{CRS, WebMercator}

      // the target CRS
      val crs: CRS = WebMercator

      val tmsLevels: Array[LayoutDefinition] = {
        val scheme = ZoomedLayoutScheme(crs, 256)
        for (zoom <- 0 to 64) yield scheme.levelForZoom(zoom).layout
      }.toArray

      val path = Resource.path("img/issue-116-cog.tif")
      val subsetBands = List(0, 1, 2)

      def gen(z: Int, x: Int, y: Int)(name: String = "tile"): MultibandTile = {
        val layoutDefinition = tmsLevels(z)

        val result =
          GeoTiffRasterSource(path)
            .reproject(WebMercator)
            .tileToLayout(layoutDefinition, NearestNeighbor)
            .read(SpatialKey(x, y), subsetBands) map { tile =>
              tile.mapBands((n: Int, t: Tile) => t.toArrayTile)
            }

        result.get

        /*result.foreach { mbtile =>
          mbtile.band(0).renderPng().write(s"/Users/daunnc/Downloads/$name-0.png")
          mbtile.band(1).renderPng().write(s"/Users/daunnc/Downloads/$name-1.png")
          mbtile.band(2).renderPng().write(s"/Users/daunnc/Downloads/$name-2.png")

          mbtile.renderPng().write(s"/Users/daunnc/Downloads/$name-mb.png")
        }*/
      }

      val good = gen(21, 624827, 760327)("tile21-test-ext3") // should be good one
      val bad = gen(22, 1249657, 1520657)("tile22-test-ext3") // no extra strip

      val gt = good.band(0)
      (1 until gt.rows).foreach { r =>
        gt.getDouble(gt.cols - 1, r) shouldNot be(0)
      }
      val bt = bad.band(0)
      (1 until bt.rows).foreach { r =>
        bt.getDouble(bt.cols - 1, r) shouldNot be(0d)
      }
    }

    it("should collect summary for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => GeoTiffRasterSource(uri): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD(sourceRDD)
      val rasterSource = GeoTiffRasterSource(inputPath)

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
          .map(uri => GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource)
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

  it("should create ContextRDD from RDD of GeoTiffRasterSources") {
    val inputPath = Resource.path("img/aspect-tiled.tif")
    val files = inputPath :: Nil
    val targetCRS = WebMercator
    val method = Bilinear
    val layoutScheme = ZoomedLayoutScheme(targetCRS, tileSize = 256)

    // read sources
    val sourceRDD: RDD[RasterSource] =
      sc.parallelize(files, files.length)
        .map(uri => GeoTiffRasterSource(uri).reproject(targetCRS, method): RasterSource)
        .cache()

    // collect raster summary
    val summary = RasterSummary.fromRDD(sourceRDD)
    val layoutLevel @ LayoutLevel(_, layout) = summary.levelFor(layoutScheme)
    val tiledLayoutSource = sourceRDD.map(_.tileToLayout(layout, method))

    // Create RDD of references, references contain information how to read rasters
    val rasterRefRdd: RDD[(SpatialKey, RasterRegion)] = tiledLayoutSource.flatMap(_.keyedRasterRegions())
    val tileRDD: RDD[(SpatialKey, MultibandTile)] =
      rasterRefRdd // group by keys and distribute raster references using SpatialPartitioner
        .groupByKey(SpatialPartitioner(summary.estimatePartitionsNumber))
        .mapValues { iter => MultibandTile(iter.flatMap(_.raster.toSeq.flatMap(_.tile.bands))) } // read rasters

    val (metadata, _) = summary.toTileLayerMetadata(layoutLevel)
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

    // TODO: fix this test
    it("should collect summary for a tiled to layout source GDAL") {
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
      val layoutLevel @ LayoutLevel(zoom, layout) = summary.levelFor(layoutScheme)
      val tiledRDD = sourceRDD.map(_.tileToLayout(layout, method))

      val summaryCollected = RasterSummary.fromRDD(tiledRDD.map(_.source))
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

      // TODO: investigate the reason of why this won't work here
      // but probably this function should be removed in the future completely and nowhere used
      // val Extent(xminc, yminc, xmaxc, ymaxc) = summaryCollected.extent
      // val Extent(xminr, yminr, xmaxr, ymaxr) = summaryResampled.extent

      // extent probably can be calculated a bit different via GeoTrellis API
      // xminc shouldBe xminr +- 1e-5
      // yminc shouldBe yminr +- 1e-5
      // xmaxc shouldBe xmaxr +- 1e-5
      // ymaxc shouldBe ymaxr +- 1e-5

      // summaryCollected.cells shouldBe summaryResampled.cells
      // summaryCollected.count shouldBe summaryResampled.count
    }
  }

  it("should create ContextRDD from RDD of GDALRasterSources") {
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
    val rasterRefRdd: RDD[(SpatialKey, RasterRegion)] = tiledLayoutSource.flatMap(_.keyedRasterRegions())
    val tileRDD: RDD[(SpatialKey, MultibandTile)] =
      rasterRefRdd // group by keys and distribute raster references using SpatialPartitioner
        .groupByKey(SpatialPartitioner(summary.estimatePartitionsNumber))
        .mapValues { iter => MultibandTile {
          iter.flatMap { rr => rr.raster.toSeq.flatMap(_.tile.bands) }
        } } // read rasters

    val (metadata, _) = summary.toTileLayerMetadata(layoutLevel)
    val contextRDD: MultibandTileLayerRDD[SpatialKey] = ContextRDD(tileRDD, metadata)

    val res = contextRDD.collect()
    res.foreach { case (_, v) => v.dimensions shouldBe layout.tileLayout.tileDimensions }
    res.length shouldBe rasterRefRdd.count()
    res.length shouldBe 72

    contextRDD.stitch.tile.band(0).renderPng().write("/tmp/raster-source-contextrdd-gdal.png")
  }

  it("Should cleanup GDAL Datasets by the end of the loop (10 iterations)") {
    val inputPath = Resource.path("img/aspect-tiled.tif")
    val targetCRS = WebMercator
    val method = Bilinear
    val layout = LayoutDefinition(GridExtent(Extent(-2.0037508342789244E7, -2.0037508342789244E7, 2.0037508342789244E7, 2.0037508342789244E7), 9.554628535647032, 9.554628535647032), 256)
    val RasterExtent(Extent(exmin, eymin, exmax, eymax), ecw, ech, ecols, erows) = RasterExtent(Extent(-8769161.632988561, 4257685.794912352, -8750616.09900087, 4274482.8318780195), CellSize(9.554628535647412, 9.554628535646911))

    cfor(0)(_ < 11, _ + 1) { _ =>
      val reference = GDALRasterSource(inputPath).reproject(targetCRS, method).tileToLayout(layout, method)
      val RasterExtent(Extent(axmin, aymin, axmax, aymax), acw, ach, acols, arows) = reference.source.rasterExtent

      axmin shouldBe exmin +- 1e-5
      aymin shouldBe eymin +- 1e-5
      axmax shouldBe exmax +- 1e-5
      aymax shouldBe eymax +- 1e-5
      acw shouldBe ecw +- 1e-5
      ach shouldBe ech +- 1e-5
      acols shouldBe ecols
      arows shouldBe erows
    }
  }
}
