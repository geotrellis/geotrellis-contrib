/*
 * Copyright 2016, 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.model

import scala.collection.mutable

/**
  * Represents grid coordinates of a subsection of a RasterExtent.
  * These coordinates are inclusive.
  */
sealed abstract class GridBounds[N: Integral: GridBounds.Ctor] extends Serializable {
  private val ord = implicitly[Ordering[N]]

  type GBWidening[In] = Widening[In, N]

  import Integral.Implicits._
  import Ordering.Implicits._

  def colMin: N
  def rowMin: N
  def colMax: N
  def rowMax: N

  def width: N = colMax - colMin + `1`
  def height: N = rowMax - rowMin + `1`
  def size: Long = width.toLong * height.toLong
  def isEmpty: Boolean = size == 0

  /**
    * Return true if the present [[GridBounds]] contains the position
    * pointed to by the given column and row, otherwise false.
    *
    * @param  col  The column
    * @param  row  The row
    */
  def contains(col: N, row: N): Boolean =
    (colMin <= col && col <= colMax) &&
      (rowMin <= row && row <= rowMax)

  /**
    * Returns true if the present [[GridBounds]] and the given one
    * intersect (including their boundaries), otherwise returns false.
    *
    * @param  other  The other GridBounds
    */
  def intersects[M: GBWidening](other: GridBounds[M]): Boolean = {
    val w = implicitly[GBWidening[M]]
    !(colMax < w(other.colMin) || w(other.colMax) < colMin) &&
    !(rowMax < w(other.rowMin) || w(other.rowMax) < rowMin)
  }

  /**
    * Creates a new [[GridBounds]] using a buffer around this
    * GridBounds.
    *
    * @note This will not buffer past 0 regardless of how much the buffer
    *       falls below it.
    * @param bufferSize The amount this GridBounds should be buffered by.
    */
  def buffer(bufferSize: N): GridBounds[N] =
    buffer(bufferSize, bufferSize)

  /**
    * Creates a new [[GridBounds]] using a buffer around this
    * GridBounds.
    *
    * @note This will not buffer past 0 regardless of how much the buffer
    *       falls below it.
    * @param colBuffer The amount the cols within this GridBounds should be buffered.
    * @param rowBuffer The amount the rows within this GridBounds should be buffered.
    * @param clamp     Determines whether or not to clamp the GridBounds to the grid
    *                  such that it no value will be under 0; defaults to true. If false,
    *                  then the resulting GridBounds can contain negative values outside
    *                  of the grid boundaries.
    */
  def buffer(colBuffer: N, rowBuffer: N, clamp: Boolean = true): GridBounds[N] = {
    GridBounds(
      if (clamp) ord.max(colMin - colBuffer, `0`) else colMin - colBuffer,
      if (clamp) ord.max(rowMin - rowBuffer, `0`) else rowMin - rowBuffer,
      colMax + colBuffer,
      rowMax + rowBuffer
    )
  }

  /**
    * Offsets this [[GridBounds]] to a new location relative to its current
    * position
    *
    * @param boundsOffset The amount the GridBounds should be shifted.
    */
  def offset(boundsOffset: N): GridBounds[N] =
    offset(boundsOffset, boundsOffset)

  /**
    * Offsets this [[GridBounds]] to a new location relative to its current
    * position
    *
    * @param colOffset The amount the cols should be shifted.
    * @param rowOffset The amount the rows should be shifted.
    */
  def offset(colOffset: N, rowOffset: N): GridBounds[N] =
    GridBounds(
      colMin + colOffset,
      rowMin + rowOffset,
      colMax + colOffset,
      rowMax + rowOffset
    )

  /**
    * Another name for the 'minus' method.
    *
    * @param  other  The other GridBounds
    */
  def -(other: GridBounds[N]): Seq[GridBounds[N]] = minus(other)

  /**
    * Returns the difference of the present [[GridBounds]] and the
    * given one.  This returns a sequence, because the difference may
    * consist of more than one GridBounds.
    *
    * @param  other  The other GridBounds
    */
  def minus(other: GridBounds[N]): Seq[GridBounds[N]] =
    if (!intersects(other)) {
      Seq(this)
    } else {
      val overlapColMin =
        if (colMin < other.colMin) other.colMin
        else colMin

      val overlapColMax =
        if (colMax < other.colMax) colMax
        else other.colMax

      val overlapRowMin =
        if (rowMin < other.rowMin) other.rowMin
        else rowMin

      val overlapRowMax =
        if (rowMax < other.rowMax) rowMax
        else other.rowMax

      val result = mutable.ListBuffer[GridBounds[N]]()
      // Left cut
      if (colMin < overlapColMin) {
        result += GridBounds(colMin, rowMin, overlapColMin - `1`, rowMax)
      }

      // Right cut
      if (overlapColMax < colMax) {
        result += GridBounds(overlapColMax + `1`, rowMin, colMax, rowMax)
      }

      // Top cut
      if (rowMin < overlapRowMin) {
        result += GridBounds(
          overlapColMin,
          rowMin,
          overlapColMax,
          overlapRowMin - `1`
        )
      }

      // Bottom cut
      if (overlapRowMax < rowMax) {
        result += GridBounds(
          overlapColMin,
          overlapRowMax + `1`,
          overlapColMax,
          rowMax
        )
      }
      result
    }
  /**
    * Return the coordinates covered by the present [[GridBounds]].
    */
  def coordsIter: Iterator[(N, N)] =
    for {
      row <- range(`0`, height, `1`)
      col <- range(`0`, width, `1`)
    } yield (col + colMin, row + rowMin)

//  /**
//    * Return the intersection of the present [[GridBounds]] and the
//    * given [[CellGrid]].
//    *
//    * @param  cellGrid  The cellGrid to intersect with
//    */
//  def intersection[M: Integral: GridBounds.Ctor](grid: Grid[M])(implicit ev2: Widening[M, N]): Option[GridBounds[N]] =
//    intersection(grid.gridBounds)

  /**
    * Return the intersection of the present [[GridBounds]] and the
    * given [[GridBounds]].
    *
    * @param  other  The other GridBounds
    */
  def intersection[M: Integral: GBWidening](other: GridBounds[M]): Option[GridBounds[N]] = {
    val widened = other.widen[N]
    if (!intersects(widened)) {
      None
    } else {
      Some(
        GridBounds(
          ord.max(colMin, widened.colMin),
          ord.max(rowMin, widened.rowMin),
          ord.min(colMax, widened.colMax),
          ord.min(rowMax, widened.rowMax)
        )
      )
    }
  }

  /** Return the union of GridBounds. */
  def combine(other: GridBounds[N]): GridBounds[N] =
    GridBounds(
      colMin = ord.min(this.colMin, other.colMin),
      rowMin = ord.min(this.rowMin, other.rowMin),
      colMax = ord.max(this.colMax, other.colMax),
      rowMax = ord.max(this.rowMax, other.rowMax)
    )

  /** Empty gridbounds contain nothing, though non empty gridbounds contains iteslf */
  def contains(other: GridBounds[N]): Boolean =
    if (colMin == 0 && colMax == 0 && rowMin == 0 && rowMax == 0) false
    else
      other.colMin >= colMin &&
      other.rowMin >= rowMin &&
      other.colMax <= colMax &&
      other.rowMax <= rowMax

  /** Split into windows, covering original GridBounds */
  def split(cols: N, rows: N): Iterator[GridBounds[N]] = {
    for {
      windowRowMin <- range(rowMin, rowMax + `1`, rows)
      windowColMin <- range(colMin, colMax + `1`, cols)
    } yield {
      GridBounds(
        colMin = windowColMin,
        rowMin = windowRowMin,
        colMax = ord.min(windowColMin + cols - `1`, colMax),
        rowMax = ord.min(windowRowMin + rows - `1`, rowMax)
      )
    }
  }

  /** Convert to a GridBound[M] */
  def widen[M: Integral: GridBounds.Ctor](implicit ev: Widening[N, M]): GridBounds[M] =
    GridBounds(ev(colMin), ev(rowMin), ev(colMax), ev(rowMax))

  /** Convert to a GridBound[M] if it can be done without numerical overflow. */
  def shrink[M: GridBounds.Ctor](implicit s: Shrinking[N, M]): Option[GridBounds[M]] = {
    for {
      cmin <- s(colMin)
      rmin <- s(rowMin)
      cmax <- s(colMax)
      rmax <- s(rowMax)
    } yield GridBounds(cmin, rmin, cmax, rmax)
  }
}

/**
  * The companion object for the [[GridBounds]] type.
  */
object GridBounds {

  def apply[N: Ctor](colMin: N, rowMin: N, colMax: N, rowMax: N): GridBounds[N] =
    Ctor[N].apply(colMin, rowMin, colMax, rowMax)

  def apply[N: Integral: Ctor](dims: Dimensions[N]): GridBounds[N] = {
    import Integral.Implicits._
    Ctor[N].apply(`0`, `0`, dims.cols - `1`, dims.rows - `1`)
  }

  /**
    * Given a sequence of keys, return a [[GridBounds]] of minimal
    * size which covers them.
    *
    * @param  keys  The sequence of keys to cover
    */
  def envelope[N: Integral: Bounded: Ctor](keys: Iterable[Product2[N, N]]): GridBounds[N] = {
    import Ordering.Implicits._
    val b = Bounded[N]

    var colMin = b.upperBound
    var colMax = b.lowerBound
    var rowMin = b.upperBound
    var rowMax = b.lowerBound

    for (key <- keys) {
      val col = key._1
      val row = key._2
      if (col < colMin) colMin = col
      if (col > colMax) colMax = col
      if (row < rowMin) rowMin = row
      if (row > rowMax) rowMax = row
    }
    GridBounds(colMin, rowMin, colMax, rowMax)
  }

  /**
    * Creates a sequence of distinct [[GridBounds]] out of a set of
    * potentially overlapping grid bounds.
    *
    * @param  gridBounds  A traversable collection of GridBounds
    */
  def distinct[N: Integral: Ordering](gridBounds: Traversable[GridBounds[N]]): Seq[GridBounds[N]] =
    gridBounds.foldLeft(Seq[GridBounds[N]]()) { (acc, bounds) =>
      acc ++ acc.foldLeft(Seq(bounds)) { (cuts, bounds) =>
        cuts.flatMap(_ - bounds)
      }
    }


  case class RasterGridBounds(colMin: Int, rowMin: Int, colMax: Int, rowMax: Int) extends GridBounds[Int]
  case class LayerGridBounds(colMin: Long, rowMin: Long, colMax: Long, rowMax: Long)  extends GridBounds[Long]

  /** Typeclass over N for constructing new instances of a GridBounds[N] */
  // TODO: this feels like something cats should already have
  trait Ctor[N] {
    def apply(colMin: N, rowMin: N, colMax: N, rowMax: N): GridBounds[N]
  }
  object Ctor {
    def apply[N: Ctor]: Ctor[N] = implicitly
    implicit val intBoundsBuilder: Ctor[Int] = new Ctor[Int]{
      def apply(colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): GridBounds[Int] =
        RasterGridBounds(colMin, rowMin, colMax, rowMax)
    }

    implicit val longBoundsBuilder: Ctor[Long] = new Ctor[Long] {
      override def apply(colMin: Long, rowMin: Long, colMax: Long, rowMax: Long): GridBounds[Long] =
        LayerGridBounds(colMin, rowMin, colMax, rowMax)
    }
  }
}
