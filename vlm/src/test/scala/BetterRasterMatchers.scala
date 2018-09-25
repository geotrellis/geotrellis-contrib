package geotrellis.contrib.vlm

import org.scalatest._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import matchers._
import spire.syntax.cfor._

import scala.reflect._

trait BetterRasterMatchers { self: Matchers with FunSpec=>
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
  }
}