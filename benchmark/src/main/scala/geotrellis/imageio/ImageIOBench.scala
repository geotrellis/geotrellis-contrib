package geotrellis.imageio

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.Blackhole

import it.geosolutions.imageio.gdalframework.GDALUtilities;
import it.geosolutions.imageio.plugins.geotiff._

import java.awt.image.{BufferedImage, Raster}
import java.awt.Rectangle
import java.io.File
import java.util.concurrent.TimeUnit

import javax.imageio.ImageReadParam
import javax.imageio.ImageReader
import javax.imageio.ImageTypeSpecifier
import javax.media.jai.ImageLayout
import javax.media.jai.JAI
import javax.media.jai.ParameterBlockJAI
import javax.media.jai.RenderedOp

import scala.collection.mutable


@State(Scope.Thread)
class ImageIOBench {

  var reader: ImageReader = null
  var image: BufferedImage = null
  val fileName = "LC08_L1GT_001003_20170921_20170921_01_RT_B1.TIF"

  @Setup(Level.Trial)
  def setup(): Unit = {
    reader = new GeoTiffImageReaderSpi().createReaderInstance()
    val cwd = (new File(".").getCanonicalPath)
    val file = new File(cwd + "/src/main/resources/" + fileName)
    file.setReadOnly
    reader.setInput(file)
    image = reader.read(0)
  }

  // Load entire 9121✕9111
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def wholeImage(blackhole: Blackhole): Unit = {
    val raster: Raster = image.getData
    blackhole.consume(raster)
  }

  // Aligned load of 512✕512 tile
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def singleAlignedTile(blackhole: Blackhole): Unit = {
    val rectangle = new Rectangle(0, 0, 512, 512)
    val raster = image.getData(rectangle)
    blackhole.consume(raster)
  }

  // Unaligned load of 512✕512 tile
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def singleUnalignedTile(blackhole: Blackhole): Unit = {
    val rectangle = new Rectangle(251, 257, 512, 512)
    val raster = image.getData(rectangle)
    blackhole.consume(raster)
  }

  // Aligned load of many 512✕512 tiles
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def manyAlignedTiles(blackhole: Blackhole): Unit = {
    val rasters = mutable.ArrayBuffer.empty[Raster]
    Range(0,9121/512).foreach({ i =>
      Range(0, 9111/512).foreach({ j =>
        val rectangle = new Rectangle(i*512, j*512, 512, 512)
        val raster = image.getData(rectangle)
        rasters.append(raster)
      })
    })
    blackhole.consume(rasters.toArray)
  }

  // Unaligned load of many 512✕512 tiles
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def manyUnalignedTiles(blackhole: Blackhole): Unit = {
    val rasters = mutable.ArrayBuffer.empty[Raster]
    Range(0,9121/512).foreach({ i =>
      Range(0, 9111/512).foreach({ j =>
        val rectangle = new Rectangle(251 + i*512, 257 + j*512, 512, 512)
        val raster = image.getData(rectangle)
        rasters.append(raster)
      })
    })
    blackhole.consume(rasters.toArray)
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
  }

}
