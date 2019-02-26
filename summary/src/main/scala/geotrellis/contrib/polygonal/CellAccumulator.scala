package geotrellis.contrib.polygonal

import simulacrum._

/** Accumulate cell values
 * Note: T instances may be mutable and re-used when adding cell values
 */
@typeclass trait CellAccumulator[T] {
  def add(self: T, v: Int): T
  def add(self: T, v: Double): T
}
