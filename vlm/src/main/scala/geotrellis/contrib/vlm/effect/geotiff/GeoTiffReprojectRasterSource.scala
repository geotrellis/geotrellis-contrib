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

import geotrellis.contrib.vlm.effect.RasterSourceF
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff.{GeoTiffMetadata, GeoTiffPath}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.vector.Extent
import geotrellis.util.RangeReader

import cats._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.list._

case class GeoTiffReprojectRasterSource[F[_]: Monad: UnsafeLift](
  dataPath: GeoTiffPath,
  targetCRS: CRS,
  targetResampleGrid: ResampleGrid[Long] = IdentityResampleGrid,
  resampleMethod: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  errorThreshold: Double = 0.125,
  private[vlm] val targetCellType: Option[TargetCellType] = None,
  private[vlm] val baseTiff: Option[F[MultibandGeoTiff]] = None
) extends RasterSourceF[F] {
  def name: GeoTiffPath = dataPath

  // memoize tiff, not useful only in a local fs case
  @transient lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(RangeReader(dataPath.value), streaming = true)
  @transient lazy val tiffF: F[MultibandGeoTiff] = baseTiff.getOrElse(UnsafeLift[F].apply(tiff))

  def bandCount: F[Int] = tiffF.map(_.bandCount)
  def cellType: F[CellType] = dstCellType.fold(tiffF.map(_.cellType))(Monad[F].pure)
  def tags: F[Tags] = tiffF.map(_.tags)
  def metadata: F[GeoTiffMetadata] = (Monad[F].pure(name), crs, bandCount, cellType, gridExtent, resolutions, tags).mapN(GeoTiffMetadata)

  /** Returns the GeoTiff head tags. */
  def attributes: F[Map[String, String]] = tags.map(_.headTags)
  /** Returns the GeoTiff per band tags. */
  def attributesForBand(band: Int): F[Map[String, String]] = tags.map(_.bandTags.lift(band).getOrElse(Map.empty))

  lazy val crs: F[CRS] = Monad[F].pure(targetCRS)
  protected lazy val baseCRS: F[CRS] = tiffF.map(_.crs)
  protected lazy val baseGridExtent: F[GridExtent[Long]] = tiffF.map(_.rasterExtent.toGridType[Long])

  // TODO: remove transient notation with Proj4 1.1 release
  @transient protected lazy val transform = (baseCRS, crs).mapN((baseCRS, crs) => Transform(baseCRS, crs))
  @transient protected lazy val backTransform = (crs, baseCRS).mapN((crs, baseCRS) => Transform(crs, baseCRS))

  override lazy val gridExtent: F[GridExtent[Long]] = {
    lazy val reprojectedRasterExtent: F[GridExtent[Long]] =
      (baseGridExtent, transform).mapN {
        ReprojectRasterExtent(_, _, Reproject.Options.DEFAULT.copy(method = resampleMethod, errorThreshold = errorThreshold) )
      }

    targetResampleGrid match {
      case IdentityResampleGrid => reprojectedRasterExtent
      case targetRegion: TargetRegion[Long] => Monad[F].pure(targetRegion.region)
      case targetGrid: TargetGrid[Long] => reprojectedRasterExtent.map(targetGrid(_))
      case dimensions: Dimensions[Long] => reprojectedRasterExtent.map(dimensions(_))
      case targetCellSize: TargetCellSize[Long] => reprojectedRasterExtent.map(targetCellSize(_))
    }
  }

  lazy val resolutions: F[List[GridExtent[Long]]] =
    (tiffF, transform).mapN { (tiff, transform) =>
      tiff.rasterExtent.toGridType[Long] ::
        tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent.toGridType[Long], transform))
    }

  @transient private[vlm] lazy val closestTiffOverview: F[GeoTiff[MultibandTile]] = {
    targetResampleGrid match {
      case IdentityResampleGrid =>
        (tiffF, baseGridExtent).mapN { (tiff, bGE) => tiff.getClosestOverview(bGE.cellSize, strategy) }
      case _ =>
        (tiffF, backTransform, gridExtent).mapN { (tiff, backTransform, gridExtent) =>
          val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
          tiff.getClosestOverview(estimatedSource.cellSize, strategy)
        }
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
    }
  }

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val bounds = gridExtent.map(_.gridBoundsFor(extent, clamp = false))
    bounds >>= (read(_, bands))
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    readBounds(List(bounds), bands) >>= { iter => closestTiffOverview.map { _.synchronized(iter.next) } }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    val bounds: F[List[GridBounds[Long]]] = extents.toList.traverse { e => gridExtent.map(_.gridBoundsFor(e, clamp = true)) }
    bounds >>= (readBounds(_, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    (closestTiffOverview, gridBounds, gridExtent, backTransform, baseCRS, crs).mapN { (closestTiffOverview, gridBounds, gridExtent, backTransform, baseCRS, crs) =>
      UnsafeLift[F].apply {
        val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]
        val intersectingWindows = {
          for {
            queryPixelBounds <- bounds
            targetPixelBounds <- queryPixelBounds.intersection(gridBounds)
          } yield {
            val targetRasterExtent = RasterExtent(
              extent = gridExtent.extentFor(targetPixelBounds, clamp = true),
              cols = targetPixelBounds.width.toInt,
              rows = targetPixelBounds.height.toInt
            )
            // A tmp workaround for https://github.com/locationtech/proj4j/pull/29
            // Stacktrace details: https://github.com/geotrellis/geotrellis-contrib/pull/206#pullrequestreview-260115791
            val sourceExtent = Proj4Transform.synchronized(targetRasterExtent.extent.reprojectAsPolygon(backTransform, 0.001).getEnvelopeInternal)
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
            resampleMethod,
            errorThreshold
          )
        }.map { convertRaster }
      }
    }.flatten

  def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(dataPath, targetCRS, resampleGrid, method, strategy, targetCellType = targetCellType, baseTiff = Some(tiffF))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(dataPath, targetCRS, resampleGrid, method, strategy, targetCellType = targetCellType, baseTiff = Some(tiffF))

  def convert(targetCellType: TargetCellType): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(dataPath, targetCRS, targetResampleGrid, resampleMethod, strategy, targetCellType = Some(targetCellType), baseTiff = Some(tiffF))
}
