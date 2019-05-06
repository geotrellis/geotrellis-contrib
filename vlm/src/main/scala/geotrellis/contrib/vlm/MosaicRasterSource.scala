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

package geotrellis.contrib.vlm

import cats.Semigroup
import cats.implicits._
import cats.data.NonEmptyList
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.merge.Implicits._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.raster.render._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.util.GetComponent
import spire.math.Integral

/**
  * Single threaded instance of a reader for reading windows out of collections
  * of rasters
  *
  * @param sources The underlying [[RasterSource]]s that you'll use for data access
  * @param crs The crs to reproject all [[RasterSource]]s to anytime we need information about their data
  * Since MosaicRasterSources represent collections of [[RasterSource]]s, we don't know in advance
  * whether they'll have the same CRS. crs allows specifying the CRS on read instead of
  * having to make sure at compile time that you're threading CRSes through everywhere correctly.
  */
trait MosaicRasterSource extends RasterSource {

  val sources: NonEmptyList[RasterSource]
  val crs: CRS
  def gridExtent: GridExtent[Long]

  import MosaicRasterSource._

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
    * The bandCount of the first [[RasterSource]] in sources
    *
    * If this value is larger than the bandCount of later [[RasterSource]]s in sources,
    * reads of all bands will fail. It is a client's responsibility to construct
    * mosaics that can be read.
    */
  def bandCount: Int = sources.head.bandCount

  def cellType: CellType = {
    val cellTypes = sources map { _.cellType }
    cellTypes.tail.foldLeft(cellTypes.head)(_ union _)
  }

  /**
    * All available resolutions for all RasterSources in this MosaicRasterSource
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.resolutions]]
    */
  def resolutions: List[GridExtent[Long]] = { val resolutions = sources map { _.resolutions }
    resolutions.reduce
  }

  /** Create a new MosaicRasterSource with sources transformed according to the provided
    * crs, options, and strategy, and a new crs
    *
    * @see [[geotrellis.contrib.vlm.RasterSource.reproject]]
    */
  def reproject(crs: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy)
      : RasterSource = MosaicRasterSource(
    sources map { _.reproject(crs, reprojectOptions, strategy) },
    crs,
    gridExtent.reproject(this.crs, crs, reprojectOptions)
  )

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters = sources map { _.read(extent, bands) }
    rasters.reduce
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters = sources map { _.read(bounds, bands) }
    rasters.reduce
  }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy)
      : RasterSource = MosaicRasterSource(
    sources map { _.resample(resampleGrid, method, strategy) },
    crs
  )

  def convert(targetCellType: TargetCellType): RasterSource = {
    MosaicRasterSource(
      sources map { _.convert(targetCellType) },
      crs
    )
  }
}

object MosaicRasterSource {
  // Orphan instance for semigroups for rasters, so we can combine
  // Option[Raster[_]]s later
  implicit val rasterSemigroup: Semigroup[Raster[MultibandTile]] =
    new Semigroup[Raster[MultibandTile]] {
      def combine(l: Raster[MultibandTile], r: Raster[MultibandTile]) = {
        val targetRE = RasterExtent(
          l.rasterExtent.extent combine r.rasterExtent.extent,
          List(l.rasterExtent.cellSize, r.rasterExtent.cellSize).minBy(_.resolution))
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

  def apply(_sources: NonEmptyList[RasterSource], _crs: CRS, _gridExtent: GridExtent[Long]) =
    new MosaicRasterSource {
      val sources = _sources map { _.reprojectToGrid(_crs, gridExtent) }
      val crs = _crs
      def gridExtent: GridExtent[Long] = _gridExtent
    }

  def apply(_sources: NonEmptyList[RasterSource], _crs: CRS) =
    new MosaicRasterSource {
      val reprojectedExtents =
        _sources map { source =>
          source.gridExtent.reproject(source.crs, _crs)
        }
      val minCellSize: CellSize = reprojectedExtents.toList map { rasterExtent =>
        CellSize(rasterExtent.cellwidth, rasterExtent.cellheight)
      } minBy { _.resolution }
      val sources = _sources map { _.reprojectToGrid(_crs, _sources.head.gridExtent) }
      val crs = _crs
      def gridExtent: GridExtent[Long] = reprojectedExtents.toList.reduce(
        (re1: GridExtent[Long], re2: GridExtent[Long]) => {
          re1.withResolution(minCellSize) combine re2.withResolution(minCellSize)
        }
      )
    }

  @SuppressWarnings(Array("TraversableHead", "TraversableTail"))
  def unsafeFromList(_sources: List[RasterSource],
                     _crs: CRS = WebMercator,
                     _gridExtent: Option[GridExtent[Long]]) =
    new MosaicRasterSource {
      val sources = NonEmptyList(_sources.head, _sources.tail)
      val crs = _crs
      def gridExtent: GridExtent[Long] = _gridExtent getOrElse { _sources.head.gridExtent}
    }
}
