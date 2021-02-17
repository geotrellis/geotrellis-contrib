/*
 * Copyright 2019 Azavea
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

package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.vector.Extent
import spire.syntax.cfor._

class DelayedMultibandTile(
  val cols: Int,
  val rows: Int,
  val cellType: CellType,
  val bandCount: Int,
  underlying: => Seq[Tile]
) extends MultibandTile with MacroMultibandCombiners {
  @transient private lazy val lazyBands: Seq[Tile] = underlying

  private def validateBand(i: Int) = require(i < bandCount, s"Band index out of bounds. Band Count: $bandCount Requested Band Index: $i")

  def band(bandIndex: Int): Tile = lazyBands(bandIndex)
  def bands: Vector[Tile] = lazyBands.toVector

  /**
    * Convert the present [[ArrayMultibandTile]] into a
    * [[MultibandTile]] with the given CellType.
    *
    * @param   newCellType  The destination CellType
    * @return               A MultibandTile of the given CellType
    */
  def convert(newCellType: CellType): MultibandTile = {
    val newBands = Array.ofDim[Tile](bandCount)
    cfor(0)(_ < bandCount, _ + 1) { i =>
      newBands(i) = band(i).convert(newCellType)
    }

    ArrayMultibandTile(newBands)
  }

  /** Return tile with cellType that reflects new NoData value */
  def withNoData(noDataValue: Option[Double]): MultibandTile =
    new ArrayMultibandTile(lazyBands.map(_.withNoData(noDataValue)).toArray)

  /** Changes the interpretation of the tile cells through changing NoData handling and optionally cell data type.
    * If [[DataType]] portion of the [[CellType]] is unchanged the tile data is not duplicated through conversion.
    * If cell [[DataType]] conversion is required it is done in a naive way, without considering NoData handling.
    *
    * @param newCellType CellType to be used in interpreting existing cells
    */
  def interpretAs(newCellType: CellType): MultibandTile =
    new ArrayMultibandTile(lazyBands.map(_.interpretAs(newCellType)).toArray)

  /**
    * Map over a subset of the bands of an [[ArrayMultibandTile]] to
    * create a new integer-valued [[MultibandTile]].
    *
    * @param   subset  A sequence containing the subset of bands that are of interest.
    * @param   f       A function to map over the bands.
    * @return          A MultibandTile containing the result
    */
  def map(subset: Seq[Int])(f: (Int, Int) => Int): MultibandTile = {
    val newBands = Array.ofDim[Tile](bandCount)
    val set = subset.toSet

    subset.foreach({ b =>
      require(0 <= b && b < bandCount, "All elements of subset must be present")
    })

    (0 until bandCount).foreach({ b =>
      if (set.contains(b))
        newBands(b) = band(b).map({ z => f(b, z) })
      else
        newBands(b) = band(b)
    })

    ArrayMultibandTile(newBands)
  }

  /**
    * Map over a subset of the bands of an [[ArrayMultibandTile]] to
    * create a new double-valued [[MultibandTile]] tile.
    *
    * @param   subset  A sequence containing the subset of bands that are of interest.
    * @param   f       A function to map over the bands.
    * @return          A MultibandTile containing the result
    */
  def mapDouble(subset: Seq[Int])(f: (Int, Double) => Double): MultibandTile = {
    val newBands = Array.ofDim[Tile](bandCount)
    val set = subset.toSet

    subset.foreach({ b =>
      require(0 <= b && b < bandCount, "All elements of subset must be present")
    })

    (0 until bandCount).foreach({ b =>
      if (set.contains(b))
        newBands(b) = band(b).mapDouble({ z => f(b, z) })
      else
        newBands(b) = band(b)
    })

    ArrayMultibandTile(newBands)
  }

  /**
    * Map each band's int value.
    *
    * @param   f  Function that takes in a band number and a value, and returns the mapped value for that cell value.
    * @return     A [[MultibandTile]] containing the result
    */
  def map(f: (Int, Int) => Int): MultibandTile = {
    val newBands = Array.ofDim[Tile](bandCount)
    cfor(0)(_ < bandCount, _ + 1) { i =>
      newBands(i) = band(i).map { z => f(i, z) }
    }

    ArrayMultibandTile(newBands)
  }

  /**
    * Map each band's double value.
    *
    * @param   f  Function that takes in a band number and a value, and returns the mapped value for that cell value.
    * @return     A [[MultibandTile]] containing the result
    */
  def mapDouble(f: (Int, Double) => Double): MultibandTile = {
    val newBands = Array.ofDim[Tile](bandCount)
    cfor(0)(_ < bandCount, _ + 1) { i =>
      newBands(i) = band(i).mapDouble { z => f(i, z) }
    }

    ArrayMultibandTile(newBands)
  }

  /**
    * Map a single band's int value.
    *
    * @param   bandIndex  Band index to map over.
    * @param   f          Function that takes in a band number and a value, and returns the mapped value for that cell value.
    * @return             A [[MultibandTile]] containing the result
    */
  def map(b0: Int)(f: Int => Int): MultibandTile = {
    validateBand(b0)
    val newBands = Array.ofDim[Tile](bandCount - 1)
    newBands(b0) = band(b0) map f
    ArrayMultibandTile(newBands)
  }

  /**
    * Map each band's double value.
    *
    * @param   f  Function that takes in a band number and a value, and returns the mapped value for that cell value.
    * @return     A [[MultibandTile]] containing the result
    */
  def mapDouble(b0: Int)(f: Double => Double): MultibandTile = {
    validateBand(b0)
    val newBands = Array.ofDim[Tile](bandCount - 1)
    newBands(b0) = band(b0) mapDouble f

    ArrayMultibandTile(newBands)
  }

  /**
    * Iterate over each band's int value.
    *
    * @param  f  Function that takes in a band number and a value, and produces some side-effect.
    */
  def foreach(f: (Int, Int) => Unit): Unit = {
    cfor(0)(_ < bandCount, _ + 1) { i =>
      band(i).foreach { z => f(i, z) }
    }
  }

  /**
    * Iterate over each band's double value.
    *
    * @param  f  Function that takes in a band number and a value, and produces some side-effect.
    */
  def foreachDouble(f: (Int, Double) => Unit): Unit = {
    cfor(0)(_ < bandCount, _ + 1) { i =>
      band(i).foreachDouble { z => f(i, z) }
    }
  }

  /**
    * Iterate over a single band's int value.
    *
    * @param  bandIndex  Band index to foreach over.
    * @param  f          Function that takes in a band number and a value, and and produces some side-effect.
    */
  def foreach(b0: Int)(f: Int => Unit): Unit = {
    validateBand(b0)
    band(b0) foreach f
  }

  /**
    * Iterate over a single band's double value.
    *
    * @param    bandIndex  Band index to foreach over.
    * @param    f          Function that takes in a band number and a value, and produces some side-effect.
    */
  def foreachDouble(b0: Int)(f: Double => Unit): Unit = {
    validateBand(b0)
    band(b0) foreachDouble f
  }

  def foreach(f: Array[Int] => Unit): Unit = {
    var i = 0
    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        val bandValues = Array.ofDim[Int](bandCount)
        cfor(0)(_ < bandCount, _ + 1) { band =>
          bandValues(band) = bands(band).get(col, row)
        }
        f(bandValues)
        i += 1
      }
    }
  }

  def foreachDouble(f: Array[Double] => Unit): Unit = {
    var i = 0
    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        val bandValues = Array.ofDim[Double](bandCount)
        cfor(0)(_ < bandCount, _ + 1) { band =>
          bandValues(band) = bands(band).getDouble(col, row)
        }
        f(bandValues)
        i += 1
      }
    }
  }

  /**
    * Combine a subset of the bands of a [[ArrayMultibandTile]] into a
    * new [[ArrayTile]] using the function f.
    *
    * @param    subset   A sequence containing the subset of bands that are of interest.
    * @param    f        A function to combine the bands.
    *
    * @return            The [[Tile]] that results from combining the bands.
    */
  def combine(subset: Seq[Int])(f: Seq[Int] => Int): Tile = {
    subset.foreach({ b => require(0 <= b && b < bandCount, "All elements of subset must be present") })
    val subsetSize = subset.size
    val subsetArray = subset.toArray

    val result = ArrayTile.empty(cellType, cols, rows)
    val values: Array[Int] = Array.ofDim(subsetSize)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        cfor(0)(_ < subsetSize, _ + 1) { i =>
          values(i) = lazyBands(subsetArray(i)).get(col, row)
        }
        result.set(col, row, f(values))
      }
    }

    result
  }

  /**
    * Combine a subset of the bands of a [[ArrayMultibandTile]] into a
    * new double-valued [[MultibandTile]] using the function f.
    *
    * @param    subset   A sequence containing the subset of bands that are of interest.
    * @param    f        A function to combine the bands.
    * @return            The [[Tile]] that results from combining the bands.
    */
  def combineDouble(subset: Seq[Int])(f: Seq[Double] => Double): Tile = {
    subset.foreach({ b => require(0 <= b && b < bandCount, "All elements of subset must be present") })
    val subsetSize = subset.size
    val subsetArray = subset.toArray

    val result = ArrayTile.empty(cellType, cols, rows)
    val values: Array[Double] = Array.ofDim(subsetSize)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        cfor(0)(_ < subsetSize, _ + 1) { i =>
          values(i) = lazyBands(subsetArray(i)).getDouble(col, row)
        }
        result.setDouble(col, row, f(values))
      }
    }
    result
  }

  /**
    * Combine each int band value for each cell.  This method will be
    * inherently slower than calling a method with explicitly stated
    * bands, so if you have as many or fewer bands to combine than an
    * explicit method call, use that.
    *
    * @param   f  A function from Array[Int] to Int.  The array contains the values of each band at a particular point.
    * @return     The [[Tile]] that results from combining the bands.
    */
  def combine(f: Array[Int] => Int): Tile = {
    val result = ArrayTile.empty(cellType, cols, rows)
    val arr = Array.ofDim[Int](bandCount)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        cfor(0)(_ < bandCount, _ + 1) { b =>
          arr(b) = band(b).get(col, row)
        }
        result.set(col, row, f(arr))
      }
    }
    result
  }

  /**
    * Combine two int band value for each cell.
    *
    * @param   b0  The index of the first band to combine.
    * @param   b1  The index of the second band to combine.
    * @param   f   A function from (Int, Int) to Int.  The tuple contains the respective values of the two bands at a particular point.
    * @return      The [[Tile]] that results from combining the bands.
    */
  def combine(b0: Int, b1: Int)(f: (Int, Int) => Int): Tile = {
    val band1 = band(b0)
    val band2 = band(b1)
    val result = ArrayTile.empty(cellType, cols, rows)

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        result.set(col, row, f(band1.get(col, row), band2.get(col, row)))
      }
    }
    result
  }

  /**
    * Combine each double band value for each cell.  This method will
    * be inherently slower than calling a method with explicitly
    * stated bands, so if you have as many or fewer bands to combine
    * than an explicit method call, use that.
    *
    * @param   f  A function from Array[Double] to Double.  The array contains the values of each band at a particular point.
    * @return     The [[Tile]] that results from combining the bands.
    */
  def combineDouble(f: Array[Double] => Double) = {
    val result = ArrayTile.empty(cellType, cols, rows)
    val arr = Array.ofDim[Double](bandCount)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        cfor(0)(_ < bandCount, _ + 1) { b =>
          arr(b) = band(b).getDouble(col, row)
        }
        result.setDouble(col, row, f(arr))
      }
    }
    result
  }

  /**
    * Combine two double band value for each cell.
    *
    * @param   b0  The index of the first band to combine.
    * @param   b1  The index of the second band to combine.
    * @param   f   A function from (Double, Double) to Double.  The tuple contains the respective values of the two bands at a particular point.
    * @return      The [[Tile]] that results from combining the bands.
    */
  def combineDouble(b0: Int, b1: Int)(f: (Double, Double) => Double): Tile = {
    val band1 = band(b0)
    val band2 = band(b1)
    val result = ArrayTile.empty(cellType, cols, rows)
    val arr = Array.ofDim[Int](bandCount)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        result.setDouble(col, row, f(band1.getDouble(col, row), band2.getDouble(col, row)))
      }
    }
    result
  }

  /**
    * Produce a new [[ArrayMultibandTile]] whose bands are taken from
    * the source ArrayMultibandTile according to the bandSequence.
    * For example, if the bandSequence is List(7,1), then the new
    * ArrayMultibandTile will have two bands, the eighth and second
    * from the source ArrayMultibandTile.
    *
    * @param  bandSequence  The list of indices to use to create the new ArrayMultibandTile.
    * @return               The resulting ArrayMultibandTile.
    */
  def subsetBands(bandSequence: Seq[Int]): ArrayMultibandTile = {
    val newBands = Array.ofDim[Tile](bandSequence.size)
    var i = 0

    require(bandSequence.size <= bandCount)
    bandSequence.foreach({ j =>
      newBands(i) = band(j)
      i += 1
    })

    new ArrayMultibandTile(newBands)
  }

  override def equals(other: Any): Boolean = other match {
    case that: ArrayMultibandTile =>
      var result = (bandCount == that.bandCount)

      cfor(0)(result && _ < bandCount, _ + 1) { i =>
        if (band(i) != that.band(i)) result = false
      }
      result
    case _ =>
      false
  }

  /**
    * Return the [[ArrayMultibandTile]] equivalent of this ArrayMultibandTile.
    *
    * @return  The object on which the method was invoked
    */
  def toArrayTile = ArrayMultibandTile(lazyBands.map(_.toArrayTile).toArray)
}
