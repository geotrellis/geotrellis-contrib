package geotrellis.contrib.vlm

import geotrellis.raster._
import geotrellis.raster.testkit.RasterMatchers
import org.scalatest._
import spire.syntax.cfor._

class PaddedTileSpec extends FunSpec with Matchers with BetterRasterMatchers with RasterMatchers {
  val padded = PaddedTile(
    chunk = IntArrayTile.fill(1, cols = 8, rows = 8),
    colOffset = 8, rowOffset = 8, cols = 16, rows = 16)

  val expected = {
    val tile = IntArrayTile.empty(16, 16)
    cfor(8)(_ < 16, _ + 1) { col =>
      cfor(8)(_ < 16, _ + 1) { row =>
        tile.set(col, row, 1)
      }
    }
    tile
  }

  it("foreach should iterate correct cell count") {
    var noDataCount = 0
    var dataCount = 0
    padded.foreach { z => if (isNoData(z)) noDataCount +=1 else dataCount += 1}

    dataCount shouldBe 8*8
    (dataCount + noDataCount) should be (16 * 16)
  }

  it("foreachDouble should iterate correct cell count") {
    var noDataCount = 0
    var dataCount = 0
    padded.foreachDouble { z => if (isNoData(z)) noDataCount +=1 else dataCount += 1}

    dataCount shouldBe 8*8
    (dataCount + noDataCount) should be (16 * 16)
  }

  it("should implement col,row,value visitor for Int") {
    padded.foreach((col, row, z) =>
      withClue(s"col = $col, row = $row") {
        z shouldBe expected.get(col, row)
      }
    )
  }

  it("should implement col,row,value visitor for Double") {
    padded.foreachDouble((col, row, z) =>
      withClue(s"col = $col, row = $row") {
        val x = expected.getDouble(col, row)
        // (Double.NaN == Double.NaN) == false
        if (isData(z) || isData(x)) z shouldBe x
      }
    )
  }

  it("should implement row-major foreach") {
    val copy = IntArrayTile.empty(16, 16)
    var i = 0
    padded.foreach{ z =>
      copy.update(i, z)
      i += 1
    }
    copy shouldBe expected
  }

  it("should implement row-major foreachDouble") {
    val copy = IntArrayTile.empty(16, 16)
    var i = 0
    padded.foreachDouble{ z =>
      copy.updateDouble(i, z)
      i += 1
    }
    copy shouldBe expected
  }
}
