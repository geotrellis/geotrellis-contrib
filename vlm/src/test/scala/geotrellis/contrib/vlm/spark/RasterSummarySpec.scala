/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.spark

import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.{BetterRasterMatchers, GlobalLayout, RasterRegion, RasterSource, Resource, TargetGrid}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._

import org.apache.spark.rdd._
import org.scalatest._

class RasterSummarySpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {

  describe("Should collect GeoTiffRasterSource RasterSummary correct") {
    it("should collect summary for a raw source") {
      val inputPath = Resource.path("img/aspect-tiled.tif")
      val files = inputPath :: Nil

      val sourceRDD: RDD[RasterSource] =
        sc.parallelize(files, files.length)
          .map(uri => GeoTiffRasterSource(uri): RasterSource)
          .cache()

      val metadata = RasterSummary.fromRDD[RasterSource, Long](sourceRDD)
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

      val summary = RasterSummary.fromRDD[RasterSource, Long](sourceRDD)
      val layoutLevel @ LayoutLevel(zoom, layout) = summary.levelFor(layoutScheme)
      val tiledLayoutSource = sourceRDD.map(_.tileToLayout(layout, method))

      val summaryCollected = RasterSummary.fromRDD[RasterSource, Long](tiledLayoutSource.map(_.source))
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
    val summary = RasterSummary.fromRDD[RasterSource, Long](sourceRDD)
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
}
