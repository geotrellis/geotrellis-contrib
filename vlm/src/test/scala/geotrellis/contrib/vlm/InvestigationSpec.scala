package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.gdal._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.wkt._
import geotrellis.raster._
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.spark.testkit._
import geotrellis.gdal._
import geotrellis.gdal.config._

import org.apache.spark.rdd.RDD

import cats.implicits._
import cats.effect.{ContextShift, IO}
import spire.syntax.cfor._
import org.scalatest._
import Inspectors._

import java.io.File
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

class InvestigationSpec extends FunSpec with TestEnvironment with BetterRasterMatchers with BeforeAndAfterAll {
  it("should compute the histogram for the layer") {
    val path = "file:///Users/daunnc/Downloads/issue-116/nlcd_2011_landcover_2011_01_18.tif"
    // val path = "file:///Users/daunnc/Downloads/issue-116/nlcd_2011_landcover_2011_02_67.tif"

    val method = NearestNeighbor
    val scheme = ZoomedLayoutScheme(WebMercator)
    val layout = scheme.levelForZoom(13).layout

    val gtSource = GeoTiffReprojectRasterSource(path, WebMercator)
    val gdSource = GDALReprojectRasterSource(path, WebMercator)
    //val tiledSource = gdSource.tileToLayout(layout)
    val tiledSource = gtSource.tileToLayout(layout)

    println(s"\n\nThis is the gridBounds of the source: ${tiledSource.source.gridBounds}")

    tiledSource.keyedRasterRegions.toArray

    println(s"\nThis is the cols and rows for GeoTiff: ${gtSource.tileToLayout(layout).source.cols}, ${gtSource.tileToLayout(layout).source.rows}")
    println(s"This is the cols and rows for GDAL: ${gdSource.tileToLayout(layout).source.cols} ${gdSource.tileToLayout(layout).source.rows}\n")

    assert(gdSource.tileToLayout(layout).keys == gtSource.tileToLayout(layout).keys)

    println(s"\nThis is the gridBounds for GeoTiff: ${gtSource.gridBounds}")
    println(s"This is the gridBounds for GDAL: ${gdSource.rasterExtent.gridBounds}\n")

    // println(s"\nThis is the sourceColOffset for GeoTiff: ${gtSource.tileToLayout(layout).sourceColOffset}")
    // println(s"This is the sourceColOffset for GDAL: ${gdSource.tileToLayout(layout).sourceColOffset}\n")

    // println(s"\nThis is the sourceRowOffset for GeoTiff: ${gtSource.tileToLayout(layout).sourceRowOffset}")
    //println(s"This is the sourceRowOffset for GDAL: ${gdSource.tileToLayout(layout).sourceRowOffset}\n")

    println(s"\nKeys for GeoTiff: ${gtSource.tileToLayout(layout).keys.size}")
    println(s"Keys for GDAL: ${gdSource.tileToLayout(layout).keys.size}\n")

    gtSource.tileToLayout(layout).keyedRasterRegions.toArray
    gdSource.tileToLayout(layout).keyedRasterRegions.toArray

    println(s"\nThis is the intersection for GeoTiff: ${gtSource.tileToLayout(layout).source.extent.intersection(layout.extent).get.toPolygon.toWKT()}")
    println(s"This is the intersection for GDAL: ${gdSource.tileToLayout(layout).source.extent.intersection(layout.extent).get.toPolygon.toWKT()}\n")
  }
}