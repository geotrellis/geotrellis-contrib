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
import geotrellis.raster.reproject._
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader


case class WarpGeoTiffRasterSource(
  uri: String,
  crs: CRS,
  resampleMethod: ResampleMethod = NearestNeighbor,
  targetRasterExtent: Option[RasterExtent] = None
) extends RasterSource {
  @transient private lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  private lazy val baseCRS = tiff.crs
  private lazy val baseRasterExtent = tiff.rasterExtent

  private lazy val transform = Transform(baseCRS, crs)
  private lazy val backTransform = Transform(crs, baseCRS)
  override lazy val rasterExtent = targetRasterExtent
    .getOrElse(ReprojectRasterExtent(baseRasterExtent, transform))

  def extent: Extent = rasterExtent.extent
  def cols: Int = rasterExtent.cols
  def rows: Int = rasterExtent.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  def readPaddedTiles(tiles: Traversable[PaddedTile], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    readBounds(tiles.map { _.targetBounds }, bands)

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = false when we have PaddedTile
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val intersectingWindows = bounds.map { targetGridBounds =>
      val targetRasterExtent = RasterExtent(rasterExtent.extentFor(targetGridBounds, clamp = false), targetGridBounds.width, targetGridBounds.height)
      val sourceExtent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
      val sourceGridBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = false)
      (sourceGridBounds, targetRasterExtent)
    }.toMap

    tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
      val targetRasterExtent = intersectingWindows(gb)
      val sourceRaster = Raster(tile, baseRasterExtent.extentFor(gb, clamp = false))

      val rr = implicitly[RasterRegionReproject[MultibandTile]]
      rr.regionReproject(
        sourceRaster,
        baseCRS,
        crs,
        targetRasterExtent,
        targetRasterExtent.extent.toPolygon,
        resampleMethod
      )
    }
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGeoTiffRasterSource =
    WarpGeoTiffRasterSource(uri, targetCRS, resampleMethod)

  def reproject(targetCRS: CRS, resampleMethod: ResampleMethod, rasterExtent: RasterExtent): RasterSource = 
    WarpGeoTiffRasterSource(uri, targetCRS, resampleMethod, Some(rasterExtent)) 
}
