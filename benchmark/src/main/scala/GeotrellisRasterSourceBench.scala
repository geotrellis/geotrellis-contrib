package geotrellis.contrib.vlm

import geotrellis.store._
import geotrellis.raster.geotiff._
import geotrellis.vector._
import geotrellis.layer._

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class GeotrellisRasterSourceBench {
  val tiffUri = "s3://geotrellis-test/eac/vlm/imgn31w092_13.tif"
  val catalogUri = "s3://geotrellis-test/eac/vlm/catalog"
  val layerId = LayerId("imgn31w092_13", 0)

  var rsGeoTiff: GeoTiffRasterSource = _
  var rsGeoTrellis: GeoTrellisRasterSource = _
  var windows: List[Extent] = Nil

  @Param(Array("100"))
  var numTiles: Int = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    rsGeoTiff = GeoTiffRasterSource(tiffUri)
    rsGeoTrellis = new GeoTrellisRasterSource(GeoTrellisPath(catalogUri, layerId.name, Some(layerId.zoom), None))

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