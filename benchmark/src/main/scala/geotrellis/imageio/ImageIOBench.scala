package geotrellis.imageio

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.Blackhole

import it.geosolutions.imageio.gdalframework.GDALUtilities;
import it.geosolutions.imageio.plugins.geotiff._

import java.awt.image.{BufferedImage, Raster}
import java.io.File

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.ParameterBlockJAI;
import javax.media.jai.RenderedOp;


@BenchmarkMode(Array(Mode.AverageTime))
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

  @Benchmark
  def imageIOWholeImage(blackhole: Blackhole): Unit = {
    val raster: Raster = image.getData
    blackhole.consume(raster)
  }

  // @Benchmark
  // def cwd(): Unit = {
  //   println(new File(".").getCanonicalPath())
  // }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
  }

}
