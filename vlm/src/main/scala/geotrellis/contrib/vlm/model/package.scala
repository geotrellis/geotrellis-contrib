package geotrellis.contrib.vlm
import geotrellis.raster.{RasterExtent => LegacyRasterExtent}
import geotrellis.raster.{GridBounds => LegacyGridBounds}
import geotrellis.raster.{GridExtent => LegacyGridExtent}
import scala.collection.Iterator.empty
import scala.collection.{AbstractIterator, Iterator}
import scala.math.Ordering

package object model {
  @inline
  private[vlm] def `1`[N: Integral] = implicitly[Integral[N]].one
  @inline
  private[vlm] def `0`[N: Integral] = implicitly[Integral[N]].one

  // TODO: Use spire for this instead?
  private[vlm] sealed trait Bounded[N] {
    def lowerBound: N
    def upperBound: N
  }
  object Bounded {
    def apply[N: Bounded]: Bounded[N] = implicitly

    implicit val intIsBounded: Bounded[Int] = new Bounded[Int] {
      override def lowerBound: Int = Int.MinValue
      override def upperBound: Int = Int.MaxValue
    }

    implicit val longIsBounded: Bounded[Long] = new Bounded[Long] {
      override def lowerBound: Long = Long.MinValue
      override def upperBound: Long = Long.MaxValue
    }
  }

  // TODO: Does spire have something?
  private[vlm] sealed trait Widening[In, Out] {
    def apply(in: In): Out
  }
  object Widening {
    def apply[In, Out](implicit ev: Widening[In, Out]): Widening[In, Out] = ev

    implicit def identityWidening[N]: Widening[N, N] = new Widening[N, N] {
      override def apply(in: N): N = in
    }

    implicit val intToLong: Widening[Int, Long] = new Widening[Int, Long] {
      override def apply(in: Int): Long = in.toLong
    }
  }

  private[vlm] sealed trait Shrinking[In, Out] {
    def apply(in: In): Option[Out]
  }
  object Shrinking {
    def apply[In, Out](implicit ev: Shrinking[In, Out]): Shrinking[In, Out] = ev
    implicit def identityShrinking[N]: Shrinking[N, N] = new Shrinking[N, N] {
      def apply(in: N): Option[N] = Some(in)
    }

    implicit val longToIntShrinking: Shrinking[Long, Int] = new Shrinking[Long, Int] {
      override def apply(in: Long): Option[Int] = in match {
        case i if i <= Int.MaxValue => Some(i.toInt)
        case _ => None
      }
    }
  }

  // TODO: does cats have something like this?
  private[vlm] def range[N: Integral](start: N, end: N, step: N): Iterator[N] =
    new AbstractIterator[N] {
      import Integral.Implicits._
      import Ordering.Implicits._

      private var i: N = start
      def hasNext: Boolean = (step <= `0` || i < end) && (step >= `0` || i > end)
      def next(): N =
        if (hasNext) { val result = i; i += step; result } else empty.next()
    }


  /** Converter to legacy RasterExtent. NB: Throws exception if conversion can't happen without overflow*/
  implicit class ToLegacyExtents[N: Integral](ge: GriddedExtent[N])(implicit s: Shrinking[N, Int]) {
    @throws[OverflowException]
    def toLegacyRasterExtent: LegacyRasterExtent = {
      (for {
        cols <- s(ge.cols)
        rows <- s(ge.rows)
      } yield LegacyRasterExtent(ge.extent, cols, rows))
      .getOrElse(throw OverflowException("Grid size is larger than maximum RasterExtent size."))
    }

    def toLegacyGridExtent: LegacyGridExtent = LegacyGridExtent(ge.extent, ge.cellSize)
  }

  implicit class ToLegacyGridBounds[N: Integral: GridBounds.Ctor](gb: GridBounds[N])(implicit s: Shrinking[N, Int]) {
    @throws[OverflowException]
      def toLegacyGridBounds: LegacyGridBounds = {
      gb.shrink[Int].map(gb => {
        LegacyGridBounds(gb.colMin, gb.rowMin, gb.colMax, gb.rowMax)
      }).getOrElse(throw OverflowException("Grid bounds are larger than maximum GridBounds size"))
    }
  }

  case class OverflowException(msg: String) extends ArithmeticException(msg)
}
