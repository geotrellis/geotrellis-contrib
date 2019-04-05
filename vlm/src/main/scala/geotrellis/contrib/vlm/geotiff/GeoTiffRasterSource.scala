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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffRasterSource(
  uri: String,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource {
  def resampleMethod: Option[ResampleMethod] = None

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  lazy val gridExtent: GridExtent[Long] = tiff.rasterExtent.toGridType[Long]
  lazy val resolutions: List[GridExtent[Long]] = gridExtent :: tiff.overviews.map(_.rasterExtent.toGridType[Long])

  def crs: CRS = tiff.crs
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffResampleRasterSource =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffRasterSource =
    GeoTiffRasterSource(uri, Some(targetCellType))

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false).toGridType[Int]
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
      // TODO: shouldn't GridExtent give me Extent for types other than N ?
      Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = false))
    }
    if (it.hasNext) Some(convertRaster(it.next)) else None
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = true))
    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val intersectingBounds: Seq[GridBounds[Int]] =
      bounds.flatMap(_.intersection(this.gridBounds)).
        toSeq.map(b => b.toGridType[Int])

    geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
      convertRaster(Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = true)))
    }
  }
}
