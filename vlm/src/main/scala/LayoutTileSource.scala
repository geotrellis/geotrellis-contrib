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

import geotrellis.raster._
import geotrellis.spark.tiling._
import geotrellis.spark.{SpatialKey}

class LayoutTileSource(val source: RasterSource, val layout: LayoutDefinition) {
  def sourceColOffset: Long = ((source.extent.xmin - layout.extent.xmin) / layout.cellwidth).toLong
  def sourceRowOffset: Long = ((layout.extent.ymax - source.extent.ymax) / layout.cellheight).toLong

  def read(key: SpatialKey): Option[MultibandTile] = {
    val col = key.col.toLong
    val row = key.row.toLong
    val sourcePixelBounds = GridBounds(
        colMin = (col * layout.tileCols - sourceColOffset).toInt, 
        rowMin = (row * layout.tileRows - sourceRowOffset).toInt, 
        colMax = ((col+1) * layout.tileCols - 1 - sourceColOffset).toInt,
        rowMax = ((row+1) * layout.tileRows - 1 - sourceRowOffset).toInt)
    for {
      bounds <- sourcePixelBounds.intersection(source)
      raster <- source.read(bounds)
    } yield { 
      // raster is smaller but not bigger than I think ...
      // its offset is relative to the raster we wished we had
      val colOffset = bounds.colMin - sourcePixelBounds.colMin
      val rowOffset = bounds.rowMin - sourcePixelBounds.rowMin
      raster.tile.mapBands{ (_, band) => 
        PaddedTile(band, colOffset, rowOffset, layout.tileCols, layout.tileRows)
      }
    }
  }


  def readAll(keys: Iterator[SpatialKey]): Iterator[(SpatialKey, MultibandTile)] = {
    for {
      key <- keys
      col = key.col.toLong
      row = key.row.toLong
      sourcePixelBounds = GridBounds(
        colMin = (col * layout.tileCols - sourceColOffset).toInt, 
        rowMin = (row * layout.tileRows - sourceRowOffset).toInt, 
        colMax = ((col+1) * layout.tileCols - 1 - sourceColOffset).toInt,
        rowMax = ((row+1) * layout.tileRows - 1 - sourceRowOffset).toInt)
      bounds <- sourcePixelBounds.intersection(source)
      raster <- source.read(bounds)
    } yield {
      // raster is smaller but not bigger than I think ...
      // its offset is relative to the raster we wished we had
      val colOffset = bounds.colMin - sourcePixelBounds.colMin
      val rowOffset = bounds.rowMin - sourcePixelBounds.rowMin
      val tile = raster.tile.mapBands{ (_, band) => 
        PaddedTile(band, colOffset, rowOffset, layout.tileCols, layout.tileRows)
      }
      (key, tile)
    }
  }
}