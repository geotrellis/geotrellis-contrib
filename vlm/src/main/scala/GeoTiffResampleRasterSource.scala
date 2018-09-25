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
import geotrellis.raster.reproject.{ReprojectRasterExtent, RasterRegionReproject, Reproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff, GeoTiffMultibandTile, AutoHigherResolution}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader


class GeoTiffResampleRasterSource(
  val uri: String,
  val resampleGrid: ResampleGrid,
  val method: ResampleMethod = NearestNeighbor
) extends RasterSource {

  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def crs: CRS = tiff.crs
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  override lazy val rasterExtent =
    resampleGrid(tiff.rasterExtent)

  @transient lazy val closetTiffOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(rasterExtent.cellSize, AutoHigherResolution)

  def reproject(targetCRS: CRS, options: Reproject.Options): GeoTiffReprojectRasterSource =
    new GeoTiffReprojectRasterSource(uri, targetCRS, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    new GeoTiffResampleRasterSource(uri, resampleGrid, method)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val targetPixelBounds = extents.map(rasterExtent.gridBoundsFor(_))
    // result extents may actually expand to cover pixels at our resolution
    // TODO: verify the logic here, should the sourcePixelBounds be calculated from input or expanded extent?
    readBounds(targetPixelBounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val geoTiffTile = closetTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]


    val windows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this)
    } yield {
      val targetExtent = rasterExtent.extentFor(targetPixelBounds)
      val sourcePixelBounds = closetTiffOverview.rasterExtent.gridBoundsFor(targetExtent, clamp = true)
      val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width, targetPixelBounds.height)

      (sourcePixelBounds, targetRasterExtent)
    }}.toMap

    geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
      val targetRasterExtent = windows(gb)
      Raster(tile, rasterExtent.extentFor(gb, clamp = true))
        .resample(targetRasterExtent, method)
    }
  }
}