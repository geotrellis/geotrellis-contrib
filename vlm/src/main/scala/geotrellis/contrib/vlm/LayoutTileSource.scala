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
import geotrellis.spark.SpatialKey

/** Reads tiles by key from a [[RasterSource]] as keyed by a [[LayoutDefinition]]
  * @note It is required that the [[RasterSource]] is pixel aligned with the [[LayoutDefinition]]
  *
  * @param source raster source that can be queried by bounding box
  * @param layout definition of a tile grid over the pixel grid
  */
class LayoutTileSource(val source: RasterSource, val layout: LayoutDefinition) extends AutoCloseable {
  LayoutTileSource.requireGridAligned(source.gridExtent, layout)

  def sourceColOffset: Long = ((source.extent.xmin - layout.extent.xmin) / layout.cellwidth).toLong
  def sourceRowOffset: Long = ((layout.extent.ymax - source.extent.ymax) / layout.cellheight).toLong

  /** Generate RasterRegion for this key */
  def rasterRegionForKey(key: SpatialKey): RasterRegion =
    RasterRegion(source, key.extent(layout).bufferByLayout(layout))

  def read(key: SpatialKey): Option[MultibandTile] =
    read(key, 0 until source.bandCount)

  /** Read tile according to key.
    * If tile area intersects source partially the non-intersecting pixels will be filled with NODATA.
    * If tile area does not intersect source None will be returned.
    */
  def read(key: SpatialKey, bands: Seq[Int]): Option[MultibandTile] = {
    val extent = key.extent(layout).bufferByLayout(layout)
    val rasterExtent = layout.createAlignedRasterExtent(extent)

    for {
      raster <- source.read(extent, bands)
    } yield {
      if (raster.tile.cols == layout.tileCols && raster.tile.rows == layout.tileRows) {
        raster.tile
      } else { // we have a raster that's smaller or larger than desired
        val gb = rasterExtent.gridBoundsFor(raster.extent)

        raster.tile.mapBands { (_, band) =>
          PaddedTile(band, gb.colMin, gb.rowMin, layout.tileCols, layout.tileRows)
        }
      }
    }
  }

  /** Read multiple tiles according to key.
    * If each tile area intersects source partially the non-intersecting pixels will be filled with NODATA.
    * If tile area does not intersect source it will be excluded from result iterator.
    */
  def readAll(keys: Iterator[SpatialKey], bands: Seq[Int]): Iterator[(SpatialKey, MultibandTile)] =
    for {
      key <- keys
      extent = key.extent(layout).bufferByLayout(layout)
      intersectingExtent <- layout.extent.intersection(extent)
      raster <- source.read(intersectingExtent, bands)
    } yield {
      val tile =
        if (raster.tile.cols == layout.tileCols && raster.tile.rows == layout.tileRows) {
          raster.tile
        } else { // we have a raster that's smaller or larger than desired
          val rasterExtent = layout.createAlignedRasterExtent(extent)
          val gb = rasterExtent.gridBoundsFor(raster.extent)

          raster.tile.mapBands { (_, band) =>
            PaddedTile(band, gb.colMin, gb.rowMin, layout.tileCols, layout.tileRows)
          }
        }
      (key, tile)
    }

  def readAll(keys: Iterator[SpatialKey]): Iterator[(SpatialKey, MultibandTile)] =
    readAll(keys, 0 until source.bandCount)

  /** Read all available tiles */
  def readAll(): Iterator[(SpatialKey, MultibandTile)] =
    readAll(keys.toIterator)

  /** Set of keys that can be read from this tile source */
  def keys: Set[SpatialKey] = {
    layout.extent.intersection(source.extent) match {
      case Some(intersection) =>
        layout.mapTransform.keysForGeometry(intersection.toPolygon)
      case None =>
        Set.empty[SpatialKey]
    }
  }

  /** All intersecting RasterRegions with their respective keys */
  def keyedRasterRegions(): Iterator[(SpatialKey, RasterRegion)] =
    keys.toIterator.map(key => (key, rasterRegionForKey(key)))

  def close = source.close()
}

object LayoutTileSource {
  def apply(source: RasterSource, layout: LayoutDefinition): LayoutTileSource =
    new LayoutTileSource(source, layout)

  private def requireGridAligned(a: GridExtent, b: GridExtent): Unit = {
    import org.scalactic._
    import TripleEquals._
    import Tolerance._

    val eps: Double = java.lang.Double.MIN_NORMAL

    @inline def isWhole(x: Double) = math.floor(x) == x

    @inline def offset(a: Double, b: Double, w: Double): Double = {
      val cols = (a - b) / w
      cols - math.floor(cols)
    }

    /**
      * TODO: This is ignored at the moment to make it soft and to make GDAL work,
      * we need to reconsider these things to be softer (?)
      */
    /*require((a.cellwidth === b.cellwidth +- eps) && (a.cellheight === b.cellheight +- eps),
      s"CellSize differs: ${a.cellSize}, ${b.cellSize}")

    require((a.extent.xmin === b.extent.xmin +- eps) || isWhole((a.extent.xmin - b.extent.xmin) / a.cellwidth),
      s"x-aligned: offset by ${a.cellSize} ${offset(a.extent.xmin, b.extent.xmin, a.cellwidth)}")

    require((a.extent.ymin === b.extent.ymin +- eps) || isWhole((a.extent.ymin - b.extent.ymin) / b.cellheight),
      s"y-aligned: offset by ${a.cellSize} ${offset(a.extent.ymin, b.extent.ymin, a.cellheight)}")*/
  }
}
