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

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.layer.LayoutDefinition
import geotrellis.util.GetComponent

import java.util.ServiceLoader

/**
  * Single threaded instance of a reader that is able to read windows from larger raster.
  * Some initilization step is expected to provide metadata about source raster
  *
  * @groupname read Read
  * @groupdesc read Functions to read windows of data from a raster source.
  * @groupprio read 0

  * @groupname resample Resample
  * @groupdesc resample Functions to resample raster data in native projection.
  * @groupprio resample 1
  *
  * @groupname reproject Reproject
  * @groupdesc reproject Functions to resample raster data in target projection.
  * @groupprio reproject 2
  */
trait RasterSource extends CellGrid[Long] with RasterMetadata with Serializable {
  def dataPath: DataPath

  /** All available RasterSource metadata */
  def metadata: RasterSourceMetadata

  protected def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource

  /** Reproject to different CRS with explicit sampling reprojectOptions.
    * @see [[geotrellis.raster.reproject.Reproject]]
    * @group reproject
    */
  def reproject(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    if (targetCRS == this.crs) this
    else reprojection(targetCRS, resampleGrid, method, strategy)


  /** Sampling grid and resolution is defined by given [[GridExtent]].
    * Resulting extent is the extent of the minimum enclosing pixel region
    *   of the data footprint in the target grid.
    * @group reproject a
    */
  def reprojectToGrid(targetCRS: CRS, grid: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    if (targetCRS == this.crs && grid == this.gridExtent) this
    else if (targetCRS == this.crs) resampleToGrid(grid, method)
    else reprojection(targetCRS, TargetGrid[Long](grid), method, strategy)

  /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
    * The extent of the result is also taken from given [[RasterExtent]],
    *   this region may be larger or smaller than the footprint of the data
    * @group reproject
    */
  def reprojectToRegion(targetCRS: CRS, region: RasterExtent, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    if (targetCRS == this.crs && region == this.gridExtent) this
    else if (targetCRS == this.crs) resampleToRegion(region.asInstanceOf[GridExtent[Long]], method)
    else reprojection(targetCRS, TargetRegion[Long](region.toGridType[Long]), method, strategy)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource

  /** Sampling grid is defined of the footprint of the data with resolution implied by column and row count.
    * @group resample
    */
  def resample(targetCols: Long, targetRows: Long, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    resample(Dimensions(targetCols, targetRows), method, strategy)

  /** Sampling grid and resolution is defined by given [[GridExtent]].
    * Resulting extent is the extent of the minimum enclosing pixel region
    *  of the data footprint in the target grid.
    * @group resample
    */
  def resampleToGrid(grid: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    resample(TargetGrid[Long](grid), method, strategy)

  /** Sampling grid and resolution is defined by given [[RasterExtent]] region.
    * The extent of the result is also taken from given [[RasterExtent]],
    *   this region may be larger or smaller than the footprint of the data
    * @group resample
    */
  def resampleToRegion(region: GridExtent[Long], method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSource =
    resample(TargetRegion[Long](region), method, strategy)

  /** Reads a window for the extent.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    * @group read
    */
  @throws[IndexOutOfBoundsException]("if requested bands do not exist")
  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]]

  /** Reads a window for pixel bounds.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    * @group read
    */
  @throws[IndexOutOfBoundsException]("if requested bands do not exist")
  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]]

  /**
    * @group read
    */
  def read(extent: Extent): Option[Raster[MultibandTile]] =
    read(extent, (0 until bandCount))

  /**
    * @group read
    */
  def read(bounds: GridBounds[Long]): Option[Raster[MultibandTile]] =
    read(bounds, (0 until bandCount))

  /**
    * @group read
    */
  def read(): Option[Raster[MultibandTile]] =
    read(extent, (0 until bandCount))

  /**
    * @group read
    */
  def read(bands: Seq[Int]): Option[Raster[MultibandTile]] =
    read(extent, bands)

  /**
    * @group read
    */
  def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(read(_, bands).toIterator)

  /**
    * @group read
    */
  def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] =
    readExtents(extents, (0 until bandCount))
  /**
    * @group read
    */
  def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(read(_, bands).toIterator)

  /**
    * @group read
    */
  def readBounds(bounds: Traversable[GridBounds[Long]]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(read(_, (0 until bandCount)).toIterator)

  /**
    * Applies the given [[LayoutDefinition]] to the source data producing a [[LayoutTileSource]].
    * In order to fit to the given layout, the source data is resampled to match the Extent
    * and CellSize of the layout.
    *
    */
  def tileToLayout(layout: LayoutDefinition, resampleMethod: ResampleMethod = NearestNeighbor): LayoutTileSource =
    LayoutTileSource(resampleToGrid(layout, resampleMethod), layout)

  private[vlm] def targetCellType: Option[TargetCellType]

  protected lazy val dstCellType: Option[CellType] =
    targetCellType match {
      case Some(target) => Some(target.cellType)
      case None => None
    }

  protected lazy val convertRaster: Raster[MultibandTile] => Raster[MultibandTile] =
    targetCellType match {
      case Some(target: ConvertTargetCellType) =>
        (raster: Raster[MultibandTile]) => target(raster)
      case _ =>
        (raster: Raster[MultibandTile]) => raster
    }

  def convert(targetCellType: TargetCellType): RasterSource

  /** Converts the values within the RasterSource from one [[CellType]] to another.
     *
     *  Note:
     *
     *  [[GDALRasterSource]] differs in how it converts data from the other RasterSources.
     *  Please see the convert docs for [[GDALRasterSource]] for more information.
     *  @group convert
     */
  def convert(targetCellType: CellType): RasterSource =
    convert(ConvertTargetCellType(targetCellType))
}

object RasterSource {
  implicit def projectedExtentComponent[T <: RasterSource]: GetComponent[T, ProjectedExtent] =
    GetComponent(rs => ProjectedExtent(rs.extent, rs.crs))

  def apply(path: String): RasterSource = {
    import scala.collection.JavaConverters._

    ServiceLoader
      .load(classOf[RasterSourceProvider])
      .iterator()
      .asScala
      .find(_.canProcess(path))
      .getOrElse(throw new RuntimeException(s"Unable to find RasterSource for $path"))
      .rasterSource(path)
  }
}
