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
import geotrellis.raster.resample.ResampleMethod
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, GeoTiffMultibandTile}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

class GeoTiffReprojectRasterSource(
  val uri: String,
  val crs: CRS,
  val options: Reproject.Options = Reproject.Options.DEFAULT
) extends RasterSource { self =>
  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  lazy val baseCRS = tiff.crs
  lazy val baseRasterExtent = tiff.rasterExtent

  lazy val transform = Transform(baseCRS, crs)
  lazy val backTransform = Transform(crs, baseCRS)
  override lazy val rasterExtent: RasterExtent = options.targetRasterExtent match {
    case Some(targetRasterExtent) =>
      targetRasterExtent
    case None =>
      ReprojectRasterExtent(baseRasterExtent, transform, options)
  }
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

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = true))
    readBounds(bounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val intersectingWindows = { for {
      queryPixelBounds <- bounds
      targetPixelBounds <- queryPixelBounds.intersection(this)
    } yield {
      val targetRasterExtent = RasterExtent(rasterExtent.extentFor(targetPixelBounds, clamp = true), targetPixelBounds.width, targetPixelBounds.height)
      val sourceExtent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
      val sourcePixelBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
      (sourcePixelBounds, targetRasterExtent)
    }}.toMap

    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    geoTiffTile.crop(intersectingWindows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
      val targetRasterExtent = intersectingWindows(sourcePixelBounds)
      val sourceRaster = Raster(tile, baseRasterExtent.extentFor(sourcePixelBounds, clamp = true))
      val rr = implicitly[RasterRegionReproject[MultibandTile]]
      rr.regionReproject(
        sourceRaster,
        baseCRS,
        crs,
        targetRasterExtent,
        targetRasterExtent.extent.toPolygon,
        options.method,
        options.errorThreshold
      )
    }
  }

  def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    new GeoTiffReprojectRasterSource(uri, targetCRS, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    new GeoTiffResampleRasterSource(uri, resampleGrid, method) {
      override def crs: CRS = self.crs
      override lazy val rasterExtent = resampleGrid(self.rasterExtent)
    }

}