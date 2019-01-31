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

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.gdal._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import org.gdal.gdal.Dataset

import java.net.MalformedURLException

import scala.collection.mutable

trait GDALBaseRasterSource extends RasterSource {
  val vsiPath: String = if (VSIPath.isVSIFormatted(uri)) uri else try {
    VSIPath(uri).vsiPath
  } catch {
    case _: Throwable =>
      throw new MalformedURLException(
        s"Invalid URI passed into the GDALRasterSource constructor: ${uri}." +
          s"Check geotrellis.contrib.vlm.gdal.VSIPath constrains, " +
          s"or pass VSI formatted String into the GDALRasterSource constructor manually."
      )
  }

  /** pointers to parent datasets to prevent them from being garbage collected */
  @transient private lazy val parentDatasets: mutable.Set[Dataset] = mutable.Set()

  /** private setters to keep things away from the user API */
  private[gdal] def addParentDataset(ds: Dataset): GDALBaseRasterSource = { parentDatasets += ds; this }
  private[gdal] def addParentDatasets(ds: Traversable[Dataset]): GDALBaseRasterSource = { parentDatasets ++= ds; this }
  private[gdal] def getParentDatasets: Set[Dataset] = parentDatasets.toSet

  /** options to override some values on transformation steps, should be used carefully as these params can change the behaviour significantly */
  val options: GDALWarpOptions
  /** options from previous transformation steps */
  val baseWarpList: List[GDALWarpOptions]
  /** current transformation options */
  val warpOptions: GDALWarpOptions
  /** the list of transformation options including the current one */
  lazy val warpList: List[GDALWarpOptions] = baseWarpList :+ warpOptions

  // generate a vrt before the current options application
  @transient lazy val fromBaseWarpList: Dataset = {
    val (ds, history) = GDAL.fromGDALWarpOptionsH(uri, baseWarpList)
    addParentDatasets(history)
    ds
  }
  // current dataset
  @transient lazy val dataset: Dataset = {
    val (ds, history) = GDAL.fromGDALWarpOptionsH(uri, warpList, fromBaseWarpList)
    addParentDatasets(history)
    ds
  }

  lazy val bandCount: Int = dataset.getRasterCount

  lazy val crs: CRS = dataset.crs.getOrElse(CRS.fromEpsgCode(4326))

  private lazy val reader: GDALReader = GDALReader(dataset)

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = fromBaseWarpList.getNoDataValue

  lazy val cellType: CellType = dataset.cellType

  lazy val rasterExtent: RasterExtent = dataset.rasterExtent

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: List[RasterExtent] = {
    val band = dataset.GetRasterBand(1)
    rasterExtent :: (0 until band.GetOverviewCount).toList.map { idx =>
      val ovr = band.GetOverview(idx)
      RasterExtent(extent, cols = ovr.getXSize, rows = ovr.getYSize)
    }
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds
      .toIterator
      .flatMap { gb => gridBounds.intersection(gb) }
      .map { gb =>
        val tile = reader.read(gb, bands = bands)
        val extent = rasterExtent.extentFor(gb)
        Raster(tile, extent)
      }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(this)), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = true when we have PaddedTile ?
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, 0 until bandCount)
  }

  override def close = dataset.delete
}
