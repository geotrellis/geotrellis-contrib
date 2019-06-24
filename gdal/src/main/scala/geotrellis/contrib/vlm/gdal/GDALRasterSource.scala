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

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import com.azavea.gdal.GDALWarp

case class GDALRasterSource(
  dataPath: GDALDataPath,
  options: GDALWarpOptions = GDALWarpOptions.EMPTY,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSource {
  val vsiPath: String = dataPath.vsiPath

  lazy val datasetType: Int = options.datasetType

  // current dataset
  @transient lazy val dataset: GDALDataset =
    GDALDataset(vsiPath, options.toWarpOptionsList.toArray)

  lazy val bandCount: Int = dataset.bandCount

  lazy val crs: CRS = dataset.crs

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = dataset.noDataValue(GDALWarp.SOURCE)

  lazy val dataType: Int = dataset.dataType

  lazy val cellType: CellType = dstCellType.getOrElse(dataset.cellType)

  lazy val gridExtent: GridExtent[Long] = dataset.rasterExtent(datasetType).toGridType[Long]

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: List[GridExtent[Long]] = dataset.resolutions(datasetType).map(_.toGridType[Long])

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds
      .toIterator
      .flatMap { gb => gridBounds.intersection(gb) }
      .map { gb =>
        val tile = dataset.readMultibandTile(gb.toGridType[Int], bands.map(_ + 1), datasetType)
        val extent = this.gridExtent.extentFor(gb)
        convertRaster(Raster(tile, extent))
      }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALRasterSource(dataPath, options.reproject(gridExtent, crs, targetCRS, reprojectOptions))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALRasterSource(dataPath, options.resample(gridExtent, resampleGrid))

  /** Converts the contents of the GDALRasterSource to the [[TargetCellType]].
   *
   *  Note:
   *
   *  GDAL handles Byte data differently than GeoTrellis. Unlike GeoTrellis,
   *  GDAL treats all Byte data as Unsigned Bytes. Thus, the output from
   *  converting to a Signed Byte CellType can result in unexpected results.
   *  When given values to convert to Byte, GDAL takes the following steps:
   *
   *  1. Checks to see if the values falls in [0, 255].
   *  2. If the value falls outside of that range, it'll clamp it so that
   *  it falls within it. For example: -1 would become 0 and 275 would turn
   *  into 255.
   *  3. If the value falls within that range and is a floating point, then
   *  GDAL will round it up. For example: 122.492 would become 122 and 64.1
   *  would become 64.
   *
   *  Thus, it is recommended that one avoids converting to Byte without first
   *  ensuring that no data will be lost.
   *
   *  Note:
   *
   *  It is not currently possible to convert to the [[BitCellType]] using GDAL.
   *  @group convert
   */
  def convert(targetCellType: TargetCellType): RasterSource = {
    /** To avoid incorrect warp cellSize transformation, we need explicitly set target dimensions. */
    GDALRasterSource(dataPath, options.convert(targetCellType, noDataValue, Some(cols.toInt -> rows.toInt)), Some(targetCellType))
  }

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(this.gridBounds)), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, 0 until bandCount)
  }
}
