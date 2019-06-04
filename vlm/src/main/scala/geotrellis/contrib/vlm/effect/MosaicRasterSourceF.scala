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

package geotrellis.contrib.vlm.effect

import geotrellis.contrib.vlm.Implicits._
import geotrellis.contrib.vlm.{ResampleGrid, TargetCellType}
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.OverviewStrategy

import cats._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.list._
import cats.data.NonEmptyList
import spire.math.Integral

/**
  * Single threaded instance of a reader for reading windows out of collections
  * of rasters
  *
  * @param sources The underlying [[RasterSourceF]]s that you'll use for data access
  * @param crs The crs to reproject all [[RasterSourceF]]s to anytime we need information about their data
  * Since MosaicRasterSources represent collections of [[RasterSourceF]]s, we don't know in advance
  * whether they'll have the same CRS. crs allows specifying the CRS on read instead of
  * having to make sure at compile time that you're threading CRSes through everywhere correctly.
  */
trait MosaicRasterSourceF[F[_]] extends RasterSourceF[F] {
  import MosaicRasterSourceF._

  val sources: NonEmptyList[F[RasterSourceF[F]]]
  val crs: F[CRS]
  def gridExtent: F[GridExtent[Long]]

  /**
    * Uri is required only for compatibility with RasterSource.
    *
    * It doesn't make sense to access "the" URI for a collection, so this throws an exception.
    */
  def uri: String = throw new NotImplementedError(
    """
      | MosaicRasterSources don't have a single uri. Perhaps you wanted the uri from
      | one of this MosaicRasterSource's sources?
    """.trim.stripMargin
  )

  val targetCellType = None

  /**
    * The bandCount of the first [[RasterSourceF]] in sources
    *
    * If this value is larger than the bandCount of later [[RasterSourceF]]s in sources,
    * reads of all bands will fail. It is a client's responsibility to construct
    * mosaics that can be read.
    */
  def bandCount: F[Int] = sources.head >>= (_.bandCount)

  def cellType: F[CellType] = {
    val cellTypes: NonEmptyList[F[CellType]] = sources map { _ >>= (_.cellType) }
    cellTypes.tail.foldLeft(cellTypes.head) { (l, r) => (l, r).mapN(_ union _) }
  }

  /**
    * All available resolutions for all RasterSources in this MosaicRasterSource
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.resolutions]]
    */
  def resolutions: F[List[GridExtent[Long]]] = {
    val resolutions: NonEmptyList[F[List[GridExtent[Long]]]] = sources map { _ >>= (_.resolutions) }
    resolutions.sequence.map(_.reduce)
  }

  /** Create a new MosaicRasterSource with sources transformed according to the provided
    * crs, options, and strategy, and a new crs
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.reproject]]
    */
  def reproject(crs: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): F[RasterSourceF[F]] =
    F.pure(MosaicRasterSourceF(
      sources map { _ >>= (_.reproject(crs, reprojectOptions, strategy)) },
      F.pure(crs),
      (gridExtent, this.crs).mapN { (gridExtent, baseCRS) => gridExtent.reproject(baseCRS, crs, reprojectOptions) }
    ))

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val rasters = sources map { _ >>= (_.read(extent, bands)) }
    rasters.sequence.map(_.reduce)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val rasters = sources map { _ >>= (_.read(bounds, bands)) }
    rasters.sequence.map(_.reduce)
  }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): F[RasterSourceF[F]] =
    F.pure(MosaicRasterSourceF(
      sources map { _ >>= (_.resample(resampleGrid, method, strategy)) },
      crs
    ))

  def convert(targetCellType: TargetCellType): F[RasterSourceF[F]] =
    F.pure(MosaicRasterSourceF(
      sources map { _ >>= (_.convert(targetCellType)) },
      crs
    ))
}

object MosaicRasterSourceF {
  // Orphan instance for semigroups for rasters, so we can combine
  // Option[Raster[_]]s later
  implicit val rasterSemigroup: Semigroup[Raster[MultibandTile]] =
  new Semigroup[Raster[MultibandTile]] {
    def combine(l: Raster[MultibandTile], r: Raster[MultibandTile]) = {
      val targetRE = RasterExtent(
        l.rasterExtent.extent combine r.rasterExtent.extent,
        List(l.rasterExtent.cellSize, r.rasterExtent.cellSize).minBy(_.resolution)
      )
      val result = l.resample(targetRE) merge r.resample(targetRE)
      result
    }
  }

  implicit def gridExtentSemigroup[N: Integral]: Semigroup[GridExtent[N]] =
    new Semigroup[GridExtent[N]] {
      def combine(l: GridExtent[N], r: GridExtent[N]): GridExtent[N] = {
        if (l.cellwidth != r.cellwidth)
          throw GeoAttrsError(s"illegal cellwidths: ${l.cellwidth} and ${r.cellwidth}")
        if (l.cellheight != r.cellheight)
          throw GeoAttrsError(s"illegal cellheights: ${l.cellheight} and ${r.cellheight}")

        val newExtent = l.extent.combine(r.extent)
        val newRows = Integral[N].fromDouble(math.round(newExtent.height / l.cellheight))
        val newCols = Integral[N].fromDouble(math.round(newExtent.width / l.cellwidth))
        new GridExtent[N](newExtent, l.cellwidth, l.cellheight, newCols, newRows)
      }
    }

  def apply[F[_]: Monad](_sources: NonEmptyList[F[RasterSourceF[F]]], _crs: F[CRS], _gridExtent: F[GridExtent[Long]]) =
    new MosaicRasterSourceF[F] {
      val F = Monad[F]
      val sources = _sources map { source =>
        (source, gridExtent, crs).mapN { (source, gridExtent, crs) =>
          source.reprojectToGrid(crs, gridExtent)
        }.flatten
      }
      val crs = _crs
      def gridExtent: F[GridExtent[Long]] = _gridExtent
    }

  def apply[F[_]: Monad](_sources: NonEmptyList[F[RasterSourceF[F]]], _crs: F[CRS]): MosaicRasterSourceF[F] =
    new MosaicRasterSourceF[F] {
      val F = Monad[F]
      val sources = _sources map { source =>
        (source, _sources.head.flatMap(_.gridExtent), _crs).mapN { (source, gridExtent, crs) =>
          source.reprojectToGrid(crs, gridExtent)
        }.flatten
      }
      val crs = _crs
      def gridExtent: F[GridExtent[Long]] = {
        val reprojectedExtents =
          _sources map { sourcef =>
            sourcef >>= { source =>
              (source.gridExtent, source.crs, _crs).mapN { (gridExtent, sourceCRS, targetCRS) =>
                gridExtent.reproject(sourceCRS, targetCRS)
              }
            }
          }
        val minCellSize =
          reprojectedExtents
            .toList
            .map { _.map { rasterExtent => CellSize(rasterExtent.cellwidth, rasterExtent.cellheight) } }
            .sequence
            .map(_.minBy(_.resolution))

        reprojectedExtents.toList.reduce { (re1: F[GridExtent[Long]], re2: F[GridExtent[Long]]) =>
          (re1, minCellSize, re2).mapN { (re1, minCellSize, re2) =>
            re1.withResolution(minCellSize) combine re2.withResolution(minCellSize)
          }
        }
      }
    }

  @SuppressWarnings(Array("TraversableHead", "TraversableTail"))
  def unsafeFromList[F[_]: Monad](
    _sources: List[F[RasterSourceF[F]]],
    _crs: CRS = WebMercator,
    _gridExtent: Option[GridExtent[Long]]
  ): MosaicRasterSourceF[F] =
    new MosaicRasterSourceF[F] {
      val F = Monad[F]
      val sources = NonEmptyList(_sources.head, _sources.tail)
      val crs = F.pure(_crs)
      def gridExtent: F[GridExtent[Long]] =
        _gridExtent.fold { _sources.head >>= (_.gridExtent) }(F.pure)
    }
}

