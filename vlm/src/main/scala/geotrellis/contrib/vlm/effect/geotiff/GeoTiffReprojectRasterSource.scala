/*
 * Copyright 2019 Azavea
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

package geotrellis.contrib.vlm.effect.geotiff

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.effect._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.vector._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import cats._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.list._

case class GeoTiffReprojectRasterSource[F[_]: Monad: UnsafeLift](
  uri: String,
  targetCRS: CRS,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSourceF[F] {
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  @transient lazy val tiffF: F[MultibandGeoTiff] = UnsafeLift[F].apply(GeoTiffReader.readMultiband(getByteReader(uri), streaming = true))

  lazy val crs: F[CRS] = Monad[F].pure(targetCRS)
  protected lazy val baseCRS: F[CRS] = tiffF.map(_.crs)
  protected lazy val baseGridExtent: F[GridExtent[Long]] = tiffF.map(_.rasterExtent.toGridType[Long])

  protected lazy val transform = (baseCRS, crs).mapN((baseCRS, crs) => Transform(baseCRS, crs))
  protected lazy val backTransform = (crs, baseCRS).mapN((crs, baseCRS) => Transform(crs, baseCRS))

  override lazy val gridExtent: F[GridExtent[Long]] = reprojectOptions.targetRasterExtent match {
    case Some(targetRasterExtent) => Monad[F].pure(targetRasterExtent.toGridType[Long])
    case None =>
      (baseGridExtent, transform).mapN { (baseGridExtent, transform) =>
        ReprojectRasterExtent(baseGridExtent, transform, reprojectOptions)
      }

  }

  lazy val resolutions: F[List[GridExtent[Long]]] =
    (tiffF, transform).mapN { (tiff, transform) =>
      tiff.rasterExtent.toGridType[Long] ::
        tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent.toGridType[Long], transform))
    }

  @transient private[vlm] lazy val closestTiffOverview: F[GeoTiff[MultibandTile]] = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      (tiffF, backTransform, gridExtent).mapN { (tiff, backTransform, gridExtent) =>
        val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
        tiff.getClosestOverview(estimatedSource.cellSize, strategy)
      }
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
    } else {
      (tiffF, baseGridExtent).mapN { (tiff, bGE) => tiff.getClosestOverview(bGE.cellSize, strategy) }
    }
  }

  def bandCount: F[Int] = tiffF.map(_.bandCount)
  def cellType: F[CellType] = dstCellType.fold(tiffF.map(_.cellType))(Monad[F].pure)

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val bounds = gridExtent.map(_.gridBoundsFor(extent, clamp = false))
    bounds.flatMap(bounds => readBounds(List(bounds), bands) >>= { iter => UnsafeLift[F].apply { iter.next } })
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    readBounds(List(bounds), bands) >>= { iter => UnsafeLift[F].apply { iter.next } }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    val bounds: F[List[GridBounds[Long]]] = extents.toList.traverse { e => gridExtent.map(_.gridBoundsFor(e, clamp = true)) }
    bounds >>= (readBounds(_, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    (closestTiffOverview, gridBounds, gridExtent, backTransform, baseCRS, crs).mapN { (closestTiffOverview, gridBounds, gridExtent, backTransform, baseCRS, crs) =>
      UnsafeLift[F].apply {
        RasterSourceF.synchronized {
          val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]
          val intersectingWindows = {
            for {
              queryPixelBounds <- bounds
              targetPixelBounds <- queryPixelBounds.intersection(gridBounds)
            } yield {
              val targetRasterExtent = RasterExtent(
                extent = gridExtent.extentFor(targetPixelBounds, clamp = true),
                cols = targetPixelBounds.width.toInt,
                rows = targetPixelBounds.height.toInt)
              val sourceExtent = targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).envelope
              val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
              (sourcePixelBounds, targetRasterExtent)
            }
          }.toMap

          geoTiffTile.crop(intersectingWindows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
            val targetRasterExtent = intersectingWindows(sourcePixelBounds)
            val sourceRaster = Raster(tile, closestTiffOverview.rasterExtent.extentFor(sourcePixelBounds, clamp = true))
            val rr = implicitly[RasterRegionReproject[MultibandTile]]
            rr.regionReproject(
              sourceRaster,
              baseCRS,
              crs,
              targetRasterExtent,
              targetRasterExtent.extent.toPolygon,
              reprojectOptions.method,
              reprojectOptions.errorThreshold
            )
          }.map {
            convertRaster
          }
        }
      }
    }.flatten

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): F[RasterSourceF[F]] =
    Monad[F].pure(GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, targetCellType))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): F[RasterSourceF[F]] =
    this.gridExtent.map { gridExtent =>
      GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions.copy(method = method, targetRasterExtent = Some(resampleGrid(gridExtent).toRasterExtent)), strategy, targetCellType)
    }

  def convert(targetCellType: TargetCellType): F[RasterSourceF[F]] =
    Monad[F].pure(GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, Some(targetCellType)))
}
