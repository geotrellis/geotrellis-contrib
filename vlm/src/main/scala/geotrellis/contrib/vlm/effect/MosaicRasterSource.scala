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

import geotrellis.contrib.vlm._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}

import cats._
import cats.syntax.parallel._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.list._
import cats.data.NonEmptyList
import cats.temp.par._
import spire.math.Integral

/**
  *
  * Single threaded instance of a reader for reading windows out of collections
  * of rasters
  *
  * @param sources The underlying [[RasterSourceF]]s that you'll use for data access
  * @param crs The crs to reproject all [[RasterSourceF]]s to anytime we need information about their data
  * Since MosaicRasterSources represent collections of [[RasterSourceF]]s, we don't know in advance
  * whether they'll have the same CRS. crs allows specifying the CRS on read instead of
  * having to make sure at compile time that you're threading CRSes through everywhere correctly.
  */
abstract class MosaicRasterSource[F[_]: Monad: Par] extends RasterSourceF[F] {
  import MosaicRasterSource._

  val sources: F[NonEmptyList[RasterSourceF[F]]]
  val crs: F[CRS]
  def gridExtent: F[GridExtent[Long]]

  val targetCellType = None

  /**
    * The bandCount of the first [[RasterSourceF]] in sources
    *
    * If this value is larger than the bandCount of later [[RasterSourceF]]s in sources,
    * reads of all bands will fail. It is a client's responsibility to construct
    * mosaics that can be read.
    */
  def bandCount: F[Int] = sources >>= (_.head.bandCount)

  def cellType: F[CellType] = {
    val cellTypes = sources >>= (_.parTraverse(_.cellType))
    cellTypes >>= (_.tail.foldLeft(cellTypes.map(_.head)) { (l, r) => (l, Monad[F].pure(r)).mapN(_ union _) })
  }

  /** All available RasterSources metadata. */
  def metadata: F[MosaicMetadata] = (Monad[F].pure(name), crs, bandCount, cellType, gridExtent, resolutions, sources >>= { _.parTraverse { _.metadata.map { case md: RasterMetadata => md } } }).mapN(MosaicMetadata)

  def attributes: F[Map[String, String]] = Monad[F].pure(Map.empty)

  def attributesForBand(band: Int): F[Map[String, String]] = Monad[F].pure(Map.empty)

  /**
    * All available resolutions for all RasterSources in this MosaicRasterSource
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.resolutions]]
    */
  def resolutions: F[List[GridExtent[Long]]] = {
    val resolutions: F[NonEmptyList[List[GridExtent[Long]]]] = sources >>= (_.parTraverse(_.resolutions))
    resolutions.map(_.reduce)
  }

  /** Create a new MosaicRasterSource with sources transformed according to the provided
    * crs, options, and strategy, and a new crs
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.reproject]]
    */
  def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): MosaicRasterSource[F] =
    MosaicRasterSource(
      sources.map(_.map { _.reproject(targetCRS, resampleGrid, method, strategy) }),
      crs,
      (gridExtent, this.crs).mapN { (gridExtent, baseCRS) => gridExtent.reproject(baseCRS, targetCRS, Reproject.Options.DEFAULT.copy(method = method)) }
    )

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] =
    sources >>= (_.parTraverse { _.read(extent, bands) }.map(_.reduce))

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    sources >>= (_.parTraverse { _.read(bounds, bands) }.map(_.reduce))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): MosaicRasterSource[F] =
    MosaicRasterSource(
      sources.map { _.map(_.resample(resampleGrid, method, strategy)) } ,
      crs
    )

  def convert(targetCellType: TargetCellType): MosaicRasterSource[F] =
    MosaicRasterSource(
      sources map { _.map(_.convert(targetCellType)) },
      crs
    )
}

object MosaicRasterSource {
  // Orphan instance for semigroups for rasters, so we can combine
  // Option[Raster[_]]s later
  implicit val rasterSemigroup: Semigroup[Raster[MultibandTile]] =
  new Semigroup[Raster[MultibandTile]] {
    def combine(l: Raster[MultibandTile], r: Raster[MultibandTile]): Raster[MultibandTile] = {
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


  def apply[F[_]: Monad: Par](
    sourcesList: F[NonEmptyList[RasterSourceF[F]]],
    targetCRS: F[CRS],
    targetGridExtent: F[GridExtent[Long]]
  ): MosaicRasterSource[F] = apply(sourcesList, targetCRS, targetGridExtent, "")

  def apply[F[_]: Monad: Par](
    sourcesList: F[NonEmptyList[RasterSourceF[F]]],
    targetCRS: F[CRS],
    targetGridExtent: F[GridExtent[Long]],
    rasterSourceName: SourceName
  ): MosaicRasterSource[F] =
    new MosaicRasterSource[F] {
      def name = rasterSourceName
      val sources = (targetCRS, targetGridExtent).mapN { case (targetCRS, targetGridExtent) =>
        sourcesList map { _.map(_.reprojectToGrid(targetCRS, targetGridExtent)) }
      }.flatten
      val crs = targetCRS
      def gridExtent: F[GridExtent[Long]] = targetGridExtent
    }

  def apply[F[_]: Monad: Par](sourcesList: F[NonEmptyList[RasterSourceF[F]]], targetCRS: F[CRS]): MosaicRasterSource[F] =
    apply(sourcesList, targetCRS, "")

  def apply[F[_]: Monad: Par](sourcesList: F[NonEmptyList[RasterSourceF[F]]], targetCRS: F[CRS], rasterSourceName: SourceName): MosaicRasterSource[F] =
    new MosaicRasterSource[F] {
      def name = rasterSourceName
      val sources = {
        (sourcesList >>= (_.head.gridExtent), targetCRS).mapN { case (gridExtent, targetCRS) =>
          sourcesList.map(_.map {
            _.reprojectToGrid(targetCRS, gridExtent)
          })
        }.flatten
      }
      val crs = targetCRS

      def gridExtent: F[GridExtent[Long]] = {
        val reprojectedExtents: F[NonEmptyList[GridExtent[Long]]] =
          sourcesList >>= { _.parTraverse(source =>
            (source.gridExtent, source.crs, targetCRS).mapN { (gridExtent, sourceCRS, targetCRS) =>
              gridExtent.reproject(sourceCRS, targetCRS)
            }) }
        val minCellSize: F[CellSize] =
          reprojectedExtents
            .map {
              _.toList
                .map { rasterExtent => CellSize(rasterExtent.cellwidth, rasterExtent.cellheight) }
                .minBy(_.resolution)
            }

        (reprojectedExtents, minCellSize).mapN { case (reprojectedExtents, minCellSize) =>
          reprojectedExtents.toList.reduce { (re1: GridExtent[Long], re2: GridExtent[Long]) =>
            re1.withResolution(minCellSize) combine re2.withResolution(minCellSize)
          }
        }
      }
    }

  @SuppressWarnings(Array("TraversableHead", "TraversableTail"))
  def unsafeFromList[F[_]: Monad: Par](
    sourcesList: List[RasterSourceF[F]],
    targetCRS: CRS = WebMercator,
    targetGridExtent: Option[GridExtent[Long]],
    rasterSourceName: SourceName = ""
  ): MosaicRasterSource[F] =
    new MosaicRasterSource[F] {
      def name = rasterSourceName
      val sources = Monad[F].pure(NonEmptyList(sourcesList.head, sourcesList.tail))
      val crs = Monad[F].pure(targetCRS)
      def gridExtent: F[GridExtent[Long]] = targetGridExtent.fold(sourcesList.head.gridExtent)(Monad[F].pure)
    }
}

