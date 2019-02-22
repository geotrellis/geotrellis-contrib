/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.model

sealed abstract class Dimensions[@specialized(Int, Long) N: Integral] extends Serializable {
  // This simulates context bounds in a trait.
  import Integral.Implicits._

  def cols: N
  def rows: N
  def size: Long = cols.toLong * rows.toLong
}
object Dimensions {
  def apply(cols: Int, rows: Int): Dimensions[Int] =
    RasterDimensions(cols, rows)
  def apply(cols: Long, rows: Long): Dimensions[Long] =
    LayerDimensions(cols, rows)

  case class RasterDimensions(cols: Int, rows: Int) extends Dimensions[Int]
  case class LayerDimensions(cols: Long, rows: Long) extends Dimensions[Long]
}
