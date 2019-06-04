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

import geotrellis.contrib.vlm.{ConvertTargetCellType, Dimensions, LayoutTileSource, ResampleGrid, TargetCellType, TargetGrid, TargetRegion}
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.reproject.Reproject
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.spark.tiling.LayoutDefinition
// import geotrellis.util.GetComponent

import cats._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.syntax.apply._
import cats.instances.list._

trait RasterSourceF[F[_]] extends Serializable {
  implicit def F: Monad[F]

  def uri: String
  def crs: F[CRS]
  def bandCount: F[Int]
  def cellType: F[CellType]
  def size: F[Long] = (cols, rows).mapN (_ * _)
  def dimensions: F[(Long, Long)] = (cols, rows).mapN((c, r) => (c, r))
  def gridBounds: F[GridBounds[Long]] = (cols, rows).mapN { case (c, r) => GridBounds(0, 0, c - 1, r - 1) }
  def cellSize: F[CellSize] = gridExtent.map(_.cellSize)
  def gridExtent: F[GridExtent[Long]]
  def resolutions: F[List[GridExtent[Long]]]
  def extent: F[Extent] = gridExtent.map(_.extent)
  def cols: F[Long] = gridExtent.map(_.cols)
  def rows: F[Long] = gridExtent.map(_.rows)

  private[vlm] def targetCellType: Option[TargetCellType]

  protected[vlm] lazy val dstCellType: Option[CellType] =
    targetCellType match {
      case Some(target) => Some(target.cellType)
      case None => None
    }

  /** Reproject to different CRS with explicit sampling reprojectOptions.
    * @see [[geotrellis.raster.reproject.Reproject]]
    * @group reproject
    */
  def reproject(crs: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): F[RasterSourceF[F]]

  /** Sampling grid is defined over the footprint of the data at default resolution
    *
    * When using this method the cell size in target CRS will be estimated such that
    * each cell in target CRS has ''approximately'' the same geographic coverage as a cell in the source CRS.
    *
    * @group reproject
    */
  def reproject(crs: CRS, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    reproject(crs, Reproject.Options(method = method), strategy)

  /** Sampling grid and resolution is defined by given [[GridExtent]].
    * Resulting extent is the extent of the minimum enclosing pixel region
    *   of the data footprint in the target grid.
    * @group reproject a
    */
  def reprojectToGrid(crs: CRS, grid: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    reproject(crs, Reproject.Options(method = method, parentGridExtent = Some(grid)), strategy)

  /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
    * The extent of the result is also taken from given [[RasterExtent]],
    *   this region may be larger or smaller than the footprint of the data
    * @group reproject
    */
  def reprojectToRegion(crs: CRS, region: RasterExtent, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    reproject(crs, Reproject.Options(method = method, targetRasterExtent = Some(region)), strategy)


  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): F[RasterSourceF[F]]

  /** Sampling grid is defined of the footprint of the data with resolution implied by column and row count.
    * @group resample
    */
  def resample(targetCols: Long, targetRows: Long, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    resample(Dimensions(targetCols, targetRows), method, strategy)

  /** Sampling grid and resolution is defined by given [[GridExtent]].
    * Resulting extent is the extent of the minimum enclosing pixel region
    *  of the data footprint in the target grid.
    * @group resample
    */
  def resampleToGrid(grid: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    resample(TargetGrid[Long](grid), method, strategy)

  /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
    * The extent of the result is also taken from given [[RasterExtent]],
    *   this region may be larger or smaller than the footprint of the data
    * @group resample
    */
  def resampleToRegion(region: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): F[RasterSourceF[F]] =
    resample(TargetRegion[Long](region), method, strategy)

  /** Reads a window for the extent.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    * @group read
    */
  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]]

  /** Reads a window for pixel bounds.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    * @group read
    */
  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]]

  /**
    * @group read
    */
  def read(extent: Extent): F[Raster[MultibandTile]] =
    bandCount >>= { bandCount => read(extent, 0 until bandCount) }


  /**
    * @group read
    */
  def read(bounds: GridBounds[Long]): F[Raster[MultibandTile]] =
    bandCount >>= { bandCount => read(bounds, 0 until bandCount) }

  /**
    * @group read
    */
  def read(): F[Raster[MultibandTile]] =
    (bandCount, extent).mapN { (bandCount, extent) => read(extent, 0 until bandCount) }.flatten

  /**
    * @group read
    */
  def read(bands: Seq[Int]): F[Raster[MultibandTile]] =
    extent >>= (read(_, bands))

  /**
    * @group read
    */
  def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    extents.toList.traverse(read(_, bands)).map(_.toIterator)

  /**
    * @group read
    */
  def readExtents(extents: Traversable[Extent]): F[Iterator[Raster[MultibandTile]]] =
    bandCount >>= { bandCount => readExtents(extents, 0 until bandCount) }
  /**
    * @group read
    */
  def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    bounds.toList.traverse(read(_, bands)).map(_.toIterator)

  /**
    * @group read
    */
  def readBounds(bounds: Traversable[GridBounds[Long]]): F[Iterator[Raster[MultibandTile]]] =
    bounds
      .toList
      .traverse { bounds => bandCount >>= { bandCount => read(bounds, 0 until bandCount) } }
      .map(_.toIterator)

  /**
    * Applies the given [[LayoutDefinition]] to the source data producing a [[LayoutTileSource]].
    * In order to fit to the given layout, the source data is resampled to match the Extent
    * and CellSize of the layout.
    *
    */
  def tileToLayout(layout: LayoutDefinition, resampleMethod: ResampleMethod = NearestNeighbor): F[LayoutTileSource] = ???
    // LayoutTileSource(resampleToGrid(layout, resampleMethod), layout)

  def convert(targetCellType: TargetCellType): F[RasterSourceF[F]]

  /** Converts the values within the RasterSource from one [[CellType]] to another.
    *
    *  Note:
    *
    *  [[GDALRasterSource]] differs in how it converts data from the other RasterSources.
    *  Please see the convert docs for [[GDALRasterSource]] for more information.
    *  @group convert
    */
  def convert(targetCellType: CellType): F[RasterSourceF[F]] =
    convert(ConvertTargetCellType(targetCellType))

  protected[vlm] lazy val convertRaster: Raster[MultibandTile] => Raster[MultibandTile] =
    targetCellType match {
      case Some(target: ConvertTargetCellType) =>
        (raster: Raster[MultibandTile]) => target(raster)
      case _ =>
        (raster: Raster[MultibandTile]) => raster
    }
}

object RasterSourceF {
  //implicit def projectedExtentComponent[F[_], T <: RasterSourceF[F]]: GetComponent[T, ProjectedExtent] =
    //GetComponent(rs => ProjectedExtent(rs.extent, rs.crs))
}
