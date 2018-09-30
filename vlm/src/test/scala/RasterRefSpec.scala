package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.reproject.Reproject

import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._

import org.apache.spark._
import org.apache.spark.rdd._
import org.scalatest._
import Inspectors._

import java.io.File

class RasterRefSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with GivenWhenThen {
  it("reads RDD of raster refs") {
    // we're going to read these and re-build gradient.tif

    Given("some files and a LayoutDefinition")
    val paths = List(
      Resource.path("img/left-to-right.tif"),
      Resource.path("img/top-to-bottom.tif"),
      Resource.path("img/diagonal.tif"))

    // I might need to discover the layout at a later stage, for now we'll know

    val layout: LayoutDefinition = {
      val scheme = FloatingLayoutScheme(32, 32)
      val tiff = GeoTiff.readSingleband(paths.head)
      scheme.levelFor(tiff.extent, tiff.cellSize).layout
    }
    val crs = LatLng

    When("Generating RDD of RasterRefs")
    val rdd: RDD[(SpatialKey, RasterRef)] with Metadata[TileLayerMetadata[SpatialKey]] = {
      val srcRdd = sc.parallelize(paths, paths.size).map { uri => new GeoTiffRasterSource(uri) }
      srcRdd.cache()

      val (combinedExtent, commonCellType) =
        srcRdd.map { src => (src.extent, src.cellType) }
          .reduce { case ((e1, ct1), (e2, ct2)) => (e1.combine(e2), ct1.union(ct2)) }

      // we know the metadata from the layout, we just need the raster extent
      val tlm = TileLayerMetadata[SpatialKey](
        cellType = commonCellType,
        layout = layout,
        extent = combinedExtent,
        crs = crs,
        bounds = KeyBounds(layout.mapTransform(combinedExtent)))

      val refRdd = srcRdd.flatMap { src =>
        // too easy? whats missing
        val tileSource = new LayoutTileSource(src, layout)
        tileSource.keys.toIterator.map { key => (key, tileSource.rasterRef(key)) }
      }

      // TADA! Jobs done.
      ContextRDD(refRdd, tlm)
      // NEXT:
      // - coembine the bands to complete the test
      // - make an app for this and run it againt WSEL rasters
      // - run aup on EMR, figure out how to set AWS secret keys?
    }

    Then("get a RasterRef for each region of each file")
    rdd.count shouldBe (8*8*3) // three 256x256 files split into 32x32 windows

    Then("convert each RasterRef to a tile")
    val realRdd: MultibandTileLayerRDD[SpatialKey] =
      rdd.withContext(_.flatMap{ case (key, ref) =>
        for {
          raster <- ref.raster
        } yield (key, raster.tile)
      })

    realRdd.count shouldBe (8*8*3) // we shouldn't have lost anything

    Then("Each row matches the layout")
    val rows = realRdd.collect
    forAll(rows) { case (key, tile) =>
      realRdd.metadata.bounds should containKey(key)
      tile should have (
        dimensions (layout.tileCols, layout.tileRows),
        cellType (realRdd.metadata.cellType)
      )
    }
  }
}