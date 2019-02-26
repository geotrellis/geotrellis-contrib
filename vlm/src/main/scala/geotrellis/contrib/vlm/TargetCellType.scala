/*
 * Copyright 2018 Azavea
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

import geotrellis.raster.{Raster, CellType, MultibandTile}

sealed trait TargetCellType {
  // this is a by name parameter, as we don't need to call the source in all ResampleGrid types
  def apply(source: => Raster[MultibandTile]): Raster[MultibandTile]
}

case class ConvertTargetCellType(cellType: CellType) extends TargetCellType {
  def apply(output: => Raster[MultibandTile]): Raster[MultibandTile] =
    output.mapTile { _.convert(cellType) }
}
