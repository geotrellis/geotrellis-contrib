package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.gdal._
import geotrellis.raster._
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

class RasterRefSpec extends FunSpec with TestEnvironment with BetterRasterMatchers {
  it("reads RDD of raster refs") {
    // we're going to read these and re-build gradient.tif
    val paths = List(
      Resource.path("/img/left-to-right.tif"),
      Resource.path("/img/top-to-bottom.tif"),
      Resource.path("/img/diagonal.tif"))

    // I might need to discover the layout at a later stage, for now we'll know
    val layout: LayoutDefinition = ???

    val rdd: RDD[(SpatialKey, RasterRef)] with Metadata[TileLayerMetadata[SpatialKey]] = {
      val refRdd =
        sc.parallelize(paths, paths.size).flatMap { uri =>
          // too easy? whats missing
          val tileSource = new LayoutTileSource(new GeoTiffRasterSource(uri), layout)
          tileSource.keys.toIterator.map { key => (key, tileSource.rasterRef(key)) }
       }
      // we may have 1800 rasters to read, so we don't want to read all the metadata on the driver
      // I guess we'll have to collect it over the RasterRefs? We will be able to cache them cheap though.
      def md: TileLayerMetadata[SpatialKey] = ???

      // TADA! Jobs done.
      ContextRDD(refRdd, md)
    }

  }
}