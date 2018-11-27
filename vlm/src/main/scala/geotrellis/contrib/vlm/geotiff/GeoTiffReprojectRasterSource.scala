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
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
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

  protected lazy val baseCRS = tiff.crs
  protected lazy val baseRasterExtent = tiff.rasterExtent

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)

  override lazy val rasterExtent: RasterExtent = options.targetRasterExtent match {
    case Some(targetRasterExtent) =>
      targetRasterExtent
    case None =>
      ReprojectRasterExtent(baseRasterExtent, transform, options)
  }
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    read(rasterExtent.gridBoundsFor(extent), bands)

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toList.flatMap(read(_, bands)).iterator

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
   val intersectingWindows =
     bounds.flatMap(_.intersection(this)).map { targetPixelBounds =>
       val targetRE = rasterExtent.rasterExtentFor(targetPixelBounds)
       val extent = targetRE.extent
       val region = ProjectedExtent(tiff.extent, tiff.crs).reprojectAsPolygon(crs, 0.001) // should not this error be configurable?

       val CellSize(dx, dy) = tiff.cellSize
       val bufferSize = 1 + (options.method match {
         case NearestNeighbor => 0
         case Bilinear => 1
         case CubicConvolution => 3
         case CubicSpline => 3
         case Lanczos => 3
         case _ => 1 // Handles aggregate resample methods?
       })

       val bufferedPreimage = extent.reproject(backTransform).expandBy(dx * bufferSize, dy * bufferSize)
       val requestRE = tiff.rasterExtent.createAlignedRasterExtent(bufferedPreimage)
       val requestGB = tiff.rasterExtent.gridBoundsFor(requestRE.extent)
       (requestGB, (targetRE, requestRE, region))
     }.toMap

    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    geoTiffTile.crop(intersectingWindows.keys.toSeq, bands.toArray).flatMap { case (requestGB, tile) =>
      val (targetRE, requestRE, region) = intersectingWindows(requestGB)

      requestRE.extent.intersection(baseRasterExtent.extent).map { _ =>
        val sourceRaster = Raster(tile, tiff.rasterExtent.extentFor(requestGB))
        implicitly[RasterRegionReproject[MultibandTile]].regionReproject(sourceRaster, tiff.crs, crs, targetRE, region, options.method)
      }
    }
  }

  def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    new GeoTiffReprojectRasterSource(uri, targetCRS, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    new GeoTiffReprojectRasterSource(uri, crs, options.copy(
      method = method,
      targetRasterExtent = Some(resampleGrid(self.rasterExtent))
    ))
}
