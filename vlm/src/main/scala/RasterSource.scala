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

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4._
import cats.effect.IO
import java.net.URI

/**
  * Single threaded instance of a reader that is able to read windows from larger raster.
  * Some initilization step is expected to provide metadata about source raster
  *
  * @groupname read Read
  * @groupdesc read Functions to read windows of data from a raster source.
  * @groupprio read 0

  * @groupname resample Resample
  * @groupdesc resample Functions to resample raster data in native projection.
  * @groupprio resample 1
  *
  * @groupname reproject Reproject
  * @groupdesc reproject Functions to resample raster data in target projection.
  * @groupprio reproject 2
  */
trait RasterSource extends Serializable {
    def uri: String
    def crs: CRS
    def bandCount: Int
    def cellType: CellType
    def cellSize = CellSize(extent, cols, rows)
    def rasterExtent: RasterExtent
    def extent: Extent = rasterExtent.extent
    def cols: Int = rasterExtent.cols
    def rows: Int = rasterExtent.rows

    /** Reproject to different CRS with explicit sampling options.
      * @see [[geotrellis.raster.reproject.Reproject]]
      * @group reproject
      */
    def reproject(crs: CRS, options: Reproject.Options): RasterSource
    
    /** Sampling grid is defined over the footprint of the data at default resolution 
      * @group reproject 
      */
    def reproject(crs: CRS, method: ResampleMethod = NearestNeighbor): RasterSource =
        reproject(crs, Reproject.Options(method = method))
    
    /** Sampling grid and resolution is defined by given [[GridExtent]].  
      * Resulting extent is the extent of the minimum enclosing pixel region 
      *   of the data footprint in the target grid.
      * @group reproject a
      */    
    def reprojectToGrid(crs: CRS, grid: GridExtent, method: ResampleMethod = NearestNeighbor): RasterSource = 
        reproject(crs, Reproject.Options(method = method, parentGridExtent = Some(grid)))

    /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
      * The extent of the result is also taken from given [[RasterExtent]],
      *   this region may be larger or smaller than the footprint of the data
      * @group reproject 
      */
    def reprojectToRegion(crs: CRS, region: RasterExtent, method: ResampleMethod = NearestNeighbor): RasterSource =
        reproject(crs, Reproject.Options(method = method, targetRasterExtent = Some(region)))
    

    def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource

    /** Sampling grid is defined of the footprint of the data with resolution implied by column and row count. 
      * @group resample
      */
    def resample(targetCols: Int, targetRows: Int, method: ResampleMethod = NearestNeighbor): RasterSource =
      resample(Dimensions(targetCols, targetRows), method)

    /** Sampling grid and resolution is defined by given [[GridExtent]].
      * Resulting extent is the extent of the minimum enclosing pixel region 
      *  of the data footprint in the target grid.
      * @group resample
      */
    def resampleToGrid(grid: GridExtent, method: ResampleMethod = NearestNeighbor): RasterSource = 
      resample(TargetGrid(grid), method)
    
    /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
      * The extent of the result is also taken from given [[RasterExtent]],
      *   this region may be larger or smaller than the footprint of the data
      * @group resample
      */
    def resampleToRegion(region: RasterExtent, method: ResampleMethod = NearestNeighbor): RasterSource = 
      resample(TargetRegion(region), method)
    
    /** Reads a window for the extent.
      * Return extent may be smaller than requested extent around raster edges.
      * May return None if the requested extent does not overlap the raster extent.
      * @group read
      */
    @throws[IndexOutOfBoundsException]("if requested bands do not exist")
    def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]]

    /** Reads a window for pixel bounds.
      * Return extent may be smaller than requested extent around raster edges.
      * May return None if the requested extent does not overlap the raster extent.
      * @group read
      */
    @throws[IndexOutOfBoundsException]("if requested bands do not exist")
    def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]]
    
    /**
      * @group read
      */
    def read(extent: Extent): Option[Raster[MultibandTile]] = 
        read(extent, (0 until bandCount))
    
    /**
      * @group read
      */
    def read(bounds: GridBounds): Option[Raster[MultibandTile]] = 
        read(bounds, (0 until bandCount))
    
    /**
      * @group read
      */
    def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
        extents.toIterator.flatMap(read(_, bands).toIterator)

    /**
      * @group read
      */
    def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] =
        extents.toIterator.flatMap(read(_, (0 until bandCount)).toIterator)
    /**
      * @group read
      */
    def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
        bounds.toIterator.flatMap(read(_, bands).toIterator)

    /**
      * @group read
      */
    def readBounds(bounds: Traversable[GridBounds]): Iterator[Raster[MultibandTile]] =
        bounds.toIterator.flatMap(read(_, (0 until bandCount)).toIterator)
}