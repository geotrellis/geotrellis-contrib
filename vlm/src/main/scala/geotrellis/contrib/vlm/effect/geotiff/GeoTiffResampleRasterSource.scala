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

import geotrellis.contrib.vlm.effect.geotiff.instances._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.effect._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._

import cats._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._

case class GeoTiffResampleRasterSource[F[_]: GeoTiffMultibandReader: UnsafeLift](
  uri: String,
  resampleGrid: ResampleGrid[Long],
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
)(implicit val F: Monad[F]) extends RasterSourceF[F] {
  def resampleMethod: Option[ResampleMethod] = Some(method)

  @transient lazy val tiffF: F[MultibandGeoTiff] = GeoTiffMultibandReader[F].read(uri)

  def crs: F[CRS] = tiffF.map(_.crs)
  def bandCount: F[Int] = tiffF.map(_.bandCount)
  def cellType: F[CellType] = dstCellType.fold(tiffF.map(_.cellType))(F.pure)

  lazy val gridExtent: F[GridExtent[Long]] = tiffF.map(_.rasterExtent.toGridType[Long])
  lazy val resolutions: F[List[GridExtent[Long]]] = {
    (tiffF, gridExtent).mapN { (tiff, gridExtent) =>
      val ratio = gridExtent.cellSize.resolution / tiff.rasterExtent.cellSize.resolution
      gridExtent :: tiff.overviews.map { ovr =>
        val re = ovr.rasterExtent
        val CellSize(cw, ch) = re.cellSize
        new GridExtent[Long](re.extent, CellSize(cw * ratio, ch * ratio))
      }
    }
  }

  @transient protected lazy val closestTiffOverview: F[GeoTiff[MultibandTile]] =
    (tiffF, gridExtent).mapN { (tiff, gridExtent) => tiff.getClosestOverview(gridExtent.cellSize, strategy) }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    new GeoTiffReprojectRasterSource[F](uri, targetCRS, reprojectOptions, strategy, targetCellType) {
      override lazy val gridExtent: F[GridExtent[Long]] = reprojectOptions.targetRasterExtent match {
        case Some(targetRasterExtent) => F.pure(targetRasterExtent.toGridType[Long])
        case None => (this.gridExtent, this.transform).mapN { (gridExtent, transform) =>
          ReprojectRasterExtent(gridExtent, transform, this.reprojectOptions)
        }
      }
    }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, Some(targetCellType))

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val bounds = gridExtent.map(_.gridBoundsFor(extent, clamp = false))
    bounds >>= (bounds => readBounds(List(bounds), bands) >>= { iter => UnsafeLift[F].apply { iter.next } })
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    readBounds(List(bounds), bands) >>= { iter => UnsafeLift[F].apply { iter.next } }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    val targetPixelBounds = gridExtent.map(gridExtent => extents.map(gridExtent.gridBoundsFor(_)))
    // result extents may actually expand to cover pixels at our resolution
    // TODO: verify the logic here, should the sourcePixelBounds be calculated from input or expanded extent?
    targetPixelBounds >>= (readBounds(_, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    (closestTiffOverview, this.gridBounds, gridExtent).mapN { (closestTiffOverview, gridBounds, gridExtent) =>
      UnsafeLift[F].apply {
        RasterSourceF.synchronized {
          val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

          val windows = {
            for {
              queryPixelBounds <- bounds
              targetPixelBounds <- queryPixelBounds.intersection(gridBounds)
            } yield {
              val targetExtent = gridExtent.extentFor(targetPixelBounds)
              val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetExtent, clamp = true)
              val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width.toInt, targetPixelBounds.height.toInt)
              (sourcePixelBounds, targetRasterExtent)
            }
          }.toMap

          geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
            val targetRasterExtent = windows(gb)
            Raster(
              tile = tile,
              extent = targetRasterExtent.extent
            ).resample(targetRasterExtent.cols, targetRasterExtent.rows, method)
          }
        }
      }
    }.flatten
}
