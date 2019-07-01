package geotrellis.contrib.vlm

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import java.io.File
import geotrellis.spark._
import geotrellis.contrib.vlm.avro._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.vector._
import geotrellis.spark.tiling._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class GeotrellisRasterSourceBench {
  val tiffUri = "s3://geotrellis-test/eac/vlm/imgn31w092_13.tif"
  val catalogUri = "s3://geotrellis-test/eac/vlm/catalog"
  val layerId = LayerId("imgn31w092_13", 0)

  var rsGeoTiff: GeoTiffRasterSource = _
  var rsGeoTrellis: GeotrellisRasterSource = _
  var windows: List[Extent] = Nil

  @Param(Array("100"))
  var numTiles: Int = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    rsGeoTiff = new GeoTiffRasterSource(tiffUri)
    rsGeoTrellis = new GeotrellisRasterSource(catalogUri, layerId)

    windows = {
      // take every 5th window over the image for test
      val scheme = FloatingLayoutScheme(256)
      val layout = scheme.levelFor(rsGeoTiff.extent, rsGeoTiff.cellSize).layout
      layout.mapTransform.
        keysForGeometry(rsGeoTiff.extent.toPolygon).
        toIterator.
        grouped(5).
        map({ keys => keys.head.extent(layout) }).
        take(100).
        toList
    }
  }

  @Benchmark
  def readGeoTiff(): Unit = {
    val tiles = windows.map(rsGeoTiff.read(_))
    require(tiles.flatten.length == windows.length)
  }

  @Benchmark
  def readGeoTrellis(): Unit = {
    val tiles = windows.map(rsGeoTrellis.read(_))
    require(tiles.flatten.length == windows.length)
  }
}