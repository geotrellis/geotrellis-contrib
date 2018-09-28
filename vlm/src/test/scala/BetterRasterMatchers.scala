package geotrellis.contrib.vlm

import org.scalatest._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.io.wkt.WKT
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import matchers._
import spire.syntax.cfor._
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.render.ascii._

import scala.reflect._
import java.nio.file.Files

trait BetterRasterMatchers { self: Matchers with FunSpec with RasterMatchers =>
  import BetterRasterMatchers._

  private def dims[T <: Grid](t: T): String =
    s"""(${t.cols}, ${t.rows})"""

  def dimensions[T<: CellGrid: ClassTag] (dims: (Int, Int)) = HavePropertyMatcher[T, (Int, Int)] { grid =>
      HavePropertyMatchResult(grid.dimensions == dims, "dimensions", dims, grid.dimensions)
  }

  def cellType[T<: CellGrid: ClassTag] (ct: CellType) = HavePropertyMatcher[T, CellType] { grid =>
      HavePropertyMatchResult(grid.cellType == ct, "cellType", ct, grid.cellType)
  }

  def bandCount(count: Int) = HavePropertyMatcher[MultibandTile, Int] { tile =>
      HavePropertyMatchResult(tile.bandCount == count, "bandCount", count, tile.bandCount)
  }

  def assertTilesEqual(actual: MultibandTile, expected: MultibandTile): Unit = {
    actual should have (
      cellType (expected.cellType),
      dimensions (expected.dimensions),
      bandCount (expected.bandCount)
    )

    withDiffRenderClue(actual, expected){
      assertEqual(actual, expected)
    }
  }

  def assertRastersEqual(actual: Raster[MultibandTile], expected: Raster[MultibandTile]): Unit = {
    actual.extent shouldBe expected.extent

    actual.tile should have (
      cellType (expected.cellType),
      dimensions (expected.dimensions),
      bandCount (expected.tile.bandCount)
    )

    withDiffRenderClue(actual.tile, expected.tile){
      assertEqual(actual.tile, expected.tile)
    }
  }

  /** Renders scaled diff tiles as a clue */
  def withDiffRenderClue[T](
    actual: MultibandTile,
    expect: MultibandTile,
    mode: DiffMode = DiffMode.DiffSum,
    eps: Double = 0,
    palette: AsciiArtEncoder.Palette = AsciiArtEncoder.Palette(" ░▒▓█"),
    size: Int = 24
  )(fun: => T) = withClue({
    require(actual.bandCount == expect.bandCount, s"Band count doesn't match: ${actual.bandCount} != ${expect.bandCount}")
    val diffs = for (b <- 0 until actual.bandCount) yield
      scaledDiff(actual.band(b), expect.band(b), mode = mode, maxDim = size, eps = eps)

    val asciiDiffs = diffs.map(_.renderAscii(palette))

    val joinedDiffs: String = asciiDiffs
      .map(_.lines.toSeq)
      .transpose
      .map(_.mkString("\t"))
      .mkString("\n")

    val bandList = (0 until actual.bandCount).mkString(",")
    val scale = s"1 char == ${actual.rows / diffs(0).rows} rows == ${actual.cols / diffs(0).cols} cols"
    s"""
    |+ Diff: band(${bandList}) @ ($scale)
    |${joinedDiffs}
    |
    """.stripMargin
  })(fun)

  def withGeoTiffClue[T](
    key: SpatialKey,
    layout: LayoutDefinition,
    actual: MultibandTile,
    expect: MultibandTile,
    crs: CRS
  )(fun: => T): T = {
    val extent = layout.mapTransform.keyToExtent(key)
    withGeoTiffClue[T](Raster(actual, extent), Raster(expect, extent), crs)(fun)
  }

  def withGeoTiffClue[T](
    actual: Raster[MultibandTile],
    expect: Raster[MultibandTile],
    crs: CRS
  )(fun: => T): T = withClue({
    val tmpDir = Files.createTempDirectory(getClass.getSimpleName)
    val actualFile = tmpDir.resolve("actual.tiff")
    val expectFile = tmpDir.resolve("expect.tiff")
    var diffFile = tmpDir.resolve("diff.tiff")
    GeoTiff(actual, crs).write(actualFile.toString, optimizedOrder = true)
    GeoTiff(expect, crs).write(expectFile.toString, optimizedOrder = true)

    if ((actual.tile.bandCount == expect.tile.bandCount) && (actual.dimensions == expect.dimensions)) {
      val diff = actual.tile.bands.zip(expect.tile.bands).map { case (l, r) => l - r }.toArray
      GeoTiff(ArrayMultibandTile(diff), actual.extent, crs).write(diffFile.toString, optimizedOrder = true)
    } else {
      diffFile = null
    }

    s"""
    |+ actual: ${actualFile}
    |+ expect: ${expectFile}
    |+ diff  : ${Option(diffFile).getOrElse("--")}
    """stripMargin
  })(fun)
}


sealed trait DiffMode {
  def apply(acc: Double, next: Double): Double
}

object DiffMode {
  case object DiffCount extends DiffMode {
    def apply(acc: Double, next: Double) = if (isNoData(acc)) 1 else acc + 1
  }
  case object DiffSum extends DiffMode {
    def apply(acc: Double, next: Double) = if (isNoData(acc)) next else acc + next
  }
  case object DiffMax extends DiffMode{
    def apply(acc: Double, next: Double) = if (isNoData(acc)) next else math.max(acc, next)
  }
  case object DiffMin extends DiffMode{
    def apply(acc: Double, next: Double) = if (isNoData(acc)) next else math.min(acc, next)
  }
}

object BetterRasterMatchers {

  def scaledDiff(actual: Tile, expect: Tile, maxDim: Int, mode: DiffMode = DiffMode.DiffSum, eps: Double = 0): Tile = {
    require(actual.dimensions == expect.dimensions,
      s"dimensions mismatch: ${actual.dimensions}, ${expect.dimensions}")

    val cols = actual.cols
    val rows = actual.rows
    val scale: Double = maxDim / math.max(cols, rows).toDouble
    val diff = ArrayTile.empty(FloatConstantNoDataCellType, (cols * scale).toInt, (rows * scale).toInt)
    val colScale: Double = diff.cols.toDouble / actual.cols.toDouble
    val rowScale: Double = diff.rows.toDouble / actual.rows.toDouble
    var diffs = 0
    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        val v1 = actual.getDouble(col, row)
        val v2 = expect.getDouble(col, row)
        val vd: Double =
          if (isNoData(v1) && isNoData(v2)) Double.NaN
          else if (isData(v1) && isNoData(v2)) math.abs(v1)
          else if (isNoData(v1) && isData(v2)) math.abs(v2)
          else math.abs(v1 - v2)

        if (isData(vd) && (vd > eps)) {
          val dcol = (colScale * col).toInt
          val drow = (rowScale * row).toInt
          val ac = diff.getDouble(dcol, drow)
          if (isData(ac)) {
            diff.setDouble(dcol, drow, ac + vd)
          } else
            diff.setDouble(dcol, drow, vd)
          diffs += 1
        }
      }
    }
    diff
  }
}