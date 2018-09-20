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
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, RasterRegionReproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, GeoTiffMultibandTile}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader


case class GeoTiffRasterSource(uri: String) extends RasterSource {
  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def extent: Extent = tiff.extent
  def crs: CRS = tiff.crs
  def cols: Int = tiff.tile.cols
  def rows: Int = tiff.tile.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): WarpGeoTiffRasterSource =
    new WarpGeoTiffRasterSource(uri, targetCRS, resampleMethod)

  def reproject(targetCRS: CRS, resampleMethod: ResampleMethod, rasterExtent: RasterExtent): RasterSource = 
    new WarpGeoTiffRasterSource(uri, targetCRS, resampleMethod, Some(rasterExtent)) 

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
    if (it.hasNext) Some(it.next) else None
  }
    
  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    geoTiffTile.crop(bounds.toSeq, bands.toArray).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
  }
}
