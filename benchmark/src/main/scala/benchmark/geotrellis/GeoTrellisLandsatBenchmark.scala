package benchmark.geotrellis

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.Blackhole

import java.io.File
import java.util.concurrent.TimeUnit

import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.Tile
import geotrellis.vector.Extent

import scala.collection.mutable


@State(Scope.Thread)
class GeoTrellisLandsatBenchmark {

  var geoTiff: SinglebandGeoTiff = null
  val fileName = "LC08_L1GT_001003_20170921_20170921_01_RT_B1.TIF"

  @Setup(Level.Trial)
  def setup(): Unit = {
    val cwd = (new File(".").getCanonicalPath)
    geoTiff = GeoTiffReader.readSingleband(cwd + "/src/main/resources/" + fileName, true)
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
  }

  // Load entire 9121✕9111
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def wholeImage(blackhole: Blackhole): Unit = {
    val tile: Tile = geoTiff.tile
    blackhole.consume(tile.toArray)
  }

  // Aligned load of 512✕512 tile
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def singleAlignedTile(blackhole: Blackhole): Unit = {
    val tile: Tile = geoTiff.crop(0, 0, 512-1, 512-1).tile
    blackhole.consume(tile.toArray)
  }

  // Unaligned load of 512✕512 tile
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def singleUnalignedTile(blackhole: Blackhole): Unit = {
    val tile: Tile = geoTiff.crop(251, 257, 251+512-1, 257+512-1).tile
    blackhole.consume(tile.toArray)
  }

  // Aligned load of many 512✕512 tiles
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def manyAlignedTiles(blackhole: Blackhole): Unit = {
    val tiles = mutable.ArrayBuffer.empty[Array[Int]]
    Range(0,9121/512).foreach({ i =>
      Range(0, 9111/512).foreach({ j =>
        val x = i*512
        val y = j*512
        val tile: Tile = geoTiff.crop(x, y, x+512-1, y+512-1).tile
        tiles.append(tile.toArray)
      })
    })
    blackhole.consume(tiles.toArray)
  }

  // Unaligned load of many 512✕512 tiles
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def manyUnalignedTiles(blackhole: Blackhole): Unit = {
    val tiles = mutable.ArrayBuffer.empty[Array[Int]]
    Range(0,9121/512).foreach({ i =>
      Range(0, 9111/512).foreach({ j =>
        val x = 251+(i*512)
        val y = 257+(j*512)
        val tile: Tile = geoTiff.crop(x, y, x+512-1, y+512-1).tile
        tiles.append(tile.toArray)
      })
    })
    blackhole.consume(tiles.toArray)
  }

}
