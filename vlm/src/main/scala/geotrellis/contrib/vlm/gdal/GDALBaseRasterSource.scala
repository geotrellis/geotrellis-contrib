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

import java.net.MalformedURLException

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.model.{GridBounds, GriddedExtent, OverflowException, Widening}
import geotrellis.gdal._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{GridBounds => LegacyGridBounds, _}
import geotrellis.vector._
import org.gdal.gdal.Dataset

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

  /** options to override some values on transformation steps, should be used carefully as these params can change the behaviour significantly */
  val options: GDALWarpOptions

  // generate a vrt before the current options application
  @transient lazy val baseDataset: Dataset = GDAL.fromGDALWarpOptions(uri, Nil)
  // current dataset
  @transient lazy val dataset: Dataset = GDAL.fromGDALWarpOptions(uri, options :: Nil, baseDataset)

  override lazy val bandCount: Int = dataset.getRasterCount

  override lazy val crs: CRS = dataset.crs.getOrElse(CRS.fromEpsgCode(4326))

  private lazy val reader: GDALReader = GDALReader(dataset)

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = baseDataset.getNoDataValue

  override lazy val cellType: CellType = dataset.cellType

  override lazy val griddedExtent: GriddedExtent[Long] = {
    val re = dataset.rasterExtent
    GriddedExtent(re.extent, re.cols, re.rows)
  }

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: List[RasterExtent] = {
    val band = dataset.GetRasterBand(1)
    val base = griddedExtent.toLegacyRasterExtent
    base :: (0 until band.GetOverviewCount).toList.map { idx =>
      val ovr = band.GetOverview(idx)
      RasterExtent(extent, cols = ovr.getXSize, rows = ovr.getYSize)
    }
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    implicit val s = implicitly[RSShrinking[Int]]
    bounds
      .toIterator
      .flatMap { gb => grid.gridBounds.intersection(gb) }
      .map { gb =>
        val lgb = gb.toLegacyGridBounds
        val tile = reader.read(lgb, bands = bands)
        val extent = griddedExtent.extentFor(gb)
        Raster(tile, extent)
      }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource = {
    val rr = griddedExtent.toLegacyRasterExtent
    val opt = options.reproject(rr, crs, targetCRS, reprojectOptions)
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, opt)
  }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options.resample(griddedExtent.toLegacyGridExtent, resampleGrid))
  }

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = griddedExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(griddedExtent.gridBounds)), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = true when we have PaddedTile ?
    val bounds = extents.map(griddedExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, 0 until bandCount)
  }

  override def close(): Unit = dataset.delete()
}
