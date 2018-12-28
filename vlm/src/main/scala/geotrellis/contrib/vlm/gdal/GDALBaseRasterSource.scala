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
import geotrellis.gdal.config.GDALCacheConfig
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import java.net.MalformedURLException

import scala.annotation.tailrec

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
  /** options from previous transformation steps */
  val baseWarpList: List[GDALWarpOptions]
  /** current transformation options */
  val warpOptions: GDALWarpOptions
  /** the list of transformation options including the current one */
  lazy val warpList: List[GDALWarpOptions] = baseWarpList :+ warpOptions

  // generate a vrt before the current options application
  @transient lazy val fromBaseWarpList: GDALDataset = GDAL.fromGDALWarpOptions(uri, baseWarpList)
  // generate a vrt with the current options application
  @transient lazy val fromWarpList: GDALDataset = GDAL.fromGDALWarpOptions(uri, warpList)

  // parent datasets, required to keep them alive and not being collected when the cache is on weak references
  @transient private lazy val parentDatasets: Array[GDALDataset] = {
    @tailrec
    def rec0(d: GDALDataset, acc: Array[GDALDataset]): Array[GDALDataset] = {
      if(d.parents.isEmpty) acc
      else rec0(d.parents.head, d.parents ++ acc) // until we don't have multiple parents for the single warp step
                                                  // parents.head will work for us
    }
    rec0(fromWarpList, Array())
  }

  @transient private lazy val parentDatasetsNumber: Int = parentDatasets.length

  // current dataset
  @transient lazy val dataset: GDALDataset = {
    if(GDALCacheConfig.valuesType.isWeak) parentDatasetsNumber
    fromWarpList
  }

  protected lazy val geoTransform: Array[Double] = dataset.geoTransform

  lazy val bandCount: Int = dataset.getRasterCount

  lazy val crs: CRS = dataset.crs.getOrElse(CRS.fromEpsgCode(4326))

  private lazy val reader: GDALReader = GDALReader(dataset)

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = fromBaseWarpList.getRasterBand(1).getNoDataValue

  lazy val cellType: CellType = {
    val (noDataValue, bufferType, typeSizeInBits) = {
      val baseBand = dataset.getRasterBand(1)

      val nd = baseBand.getNoDataValue
      val bufferType = baseBand.getDataType
      val typeSizeInBits = sgdal.getDataTypeSize(bufferType)
      (nd, bufferType, Some(typeSizeInBits))
    }
    GDALUtils.dataTypeToCellType(bufferType, noDataValue, typeSizeInBits)
  }

  lazy val rasterExtent: RasterExtent = {
    val colsLong: Long = dataset.getRasterXSize
    val rowsLong: Long = dataset.getRasterYSize

    val cols: Int = colsLong.toInt
    val rows: Int = rowsLong.toInt

    val xmin: Double = geoTransform(0)
    val ymin: Double = geoTransform(3) + geoTransform(5) * rows
    val xmax: Double = geoTransform(0) + geoTransform(1) * cols
    val ymax: Double = geoTransform(3)

    RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
  }

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: List[RasterExtent] = {
    val band = dataset.getRasterBand(1)
    rasterExtent :: (0 until band.getOverviewCount).toList.map { idx =>
      val ovr = band.getOverview(idx)
      RasterExtent(extent, cols = ovr.getXSize, rows = ovr.getYSize)
    }
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val tuples =
      bounds.map { gb =>
        val re = rasterExtent.rasterExtentFor(gb)
        val boundsClamped = rasterExtent.gridBoundsFor(re.extent, clamp = true)
        (gb, boundsClamped, re)
      }

    tuples.map { case (gb, gbc, re) =>
      val initialTile = reader.read(gb, bands = bands)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val updatedTiles = initialTile.bands.map { band =>
            // TODO: it can't be larger than the source is, fix it
            val protoTile = band.prototype(re.cols, re.rows)

            protoTile.update(gb.colMin - gbc.colMin, gb.rowMin - gbc.rowMin, band)
            protoTile
          }

          (gb, MultibandTile(updatedTiles))
        } else (gbc, initialTile)

      Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
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
