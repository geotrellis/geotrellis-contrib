package geotrellis.contrib.polygonal

import simulacrum._

/** Visitor for  cell values of T that may record its state in R
 * Note: R instances may be mutable and re-used when adding cell values
 */
trait CellVisitor[T, R] {
  def register(raster: T, col: Int, row: Int, acc: R): R
}

object CellVisitor {
  def apply[T, R](implicit ev: CellVisitor[T, R]) = ev
}
