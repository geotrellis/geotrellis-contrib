package geotrellis.contrib.vlm

import org.scalatest._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import matchers._
import spire.syntax.cfor._
import geotrellis.raster.testkit.RasterMatchers

import scala.reflect._

trait BetterRasterMatchers { self: Matchers with FunSpec with RasterMatchers =>
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

  /** Renders scaled diff tiles as a clue */
  def withDiffRenderClue[T](actual: MultibandTile, expect: MultibandTile)(fun: => T) = {
    require(actual.bandCount == expect.bandCount, s"Band count doesn't match: ${actual.bandCount} != ${expect.bandCount}")
    val asciiDiffs = for (b <- 0 until actual.bandCount) yield
      scaledDiff(actual.band(b), expect.band(b), maxDim = 20).renderAscii()

    val joinedDiffs: String = asciiDiffs
      .map(_.lines.toSeq)
      .transpose
      .map(_.mkString("\t"))
      .mkString("\n")

    val bandList = (0 until actual.bandCount).mkString(",")

    withClue(s"""
    |Band(${bandList}) diff:
    |${joinedDiffs}
    |
    """.stripMargin)(fun)
  }

  /** Create a scale tile where no dimension exceeds max value */
  private def scaledTile(model: CellGrid, max: Int)(cellType: CellType = model.cellType): MutableArrayTile = {
    val cols = model.cols
    val rows = model.rows

    val (scaledCols, scaledRows) =
      if ((cols > rows) && cols > max)
        (max, (rows * (max.toDouble / cols)).toInt)
      else if (rows > max)
        ((cols * (max.toDouble / rows)).toInt, rows)
      else
        (cols, rows)

    ArrayTile.empty(cellType, scaledCols, scaledRows)
  }

  def scaledDiff(actual: Tile, expect: Tile, maxDim: Int, eps: Double = Double.MinPositiveValue): Tile = {
    // TODO: Add DiffMode (change count, accumulated value diff, change flag)
    require(actual.cols == expect.cols)
    require(actual.rows == expect.rows)
    val cols = actual.cols
    val rows = actual.rows
    val diff = scaledTile(actual, maxDim)(FloatConstantNoDataCellType)
    val colScale: Double = diff.cols.toDouble / actual.cols.toDouble
    val rowScale: Double = diff.rows.toDouble / actual.rows.toDouble
    var diffs = 0
    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        val v1 = actual.getDouble(col, row)
        val v2 = expect.getDouble(col, row)
          val vd = math.abs(math.abs(v1) - math.abs(v2))
          if (! (v1.isNaN && v2.isNaN) || (vd > eps)) {
            val dcol = (colScale * col).toInt
            val drow = (rowScale * row).toInt
            val ac = diff.getDouble(dcol, drow)
            if (isData(ac)) {
              diff.setDouble(dcol, drow, ac + 1)
            } else
              diff.setDouble(dcol, drow, 1)
            diffs += 1
          }
      }
    }
    diff
  }


}