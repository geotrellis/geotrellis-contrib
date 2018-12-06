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
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference

trait GDALBaseRasterSource extends RasterSource {
  /** Aligns the coordinates of the extent of the output file to the values of the target resolution,
    * such that the aligned extent includes the minimum extent.
    *
    * In terms of GeoTrellis it's very similar to [[GridExtent]].createAlignedGridExtent:
    *
    * newMinX = floor(envelop.minX / xRes) * xRes
    * newMaxX = ceil(envelop.maxX / xRes) * xRes
    * newMinY = floor(envelop.minY / yRes) * yRes
    * newMaxY = ceil(envelop.maxY / yRes) * yRes
    *
    * if (xRes == 0) || (yRes == 0) than GDAL calculates it using the extent and the cellSize
    * xRes = (maxX - minX) / cellSize.width
    * yRes = (maxY - minY) / cellSize.height
    *
    * If -tap parameter is NOT set, GDAL increases extent by a half of a pixel, to avoid missing points on the border.
    *
    * The actual code reference: https://github.com/OSGeo/gdal/blob/v2.3.2/gdal/apps/gdal_rasterize_lib.cpp#L402-L461
    * The actual part with the -tap logic: https://github.com/OSGeo/gdal/blob/v2.3.2/gdal/apps/gdal_rasterize_lib.cpp#L455-L461
    *
    * The initial PR that introduced that feature in GDAL 1.8.0: https://trac.osgeo.org/gdal/attachment/ticket/3772/gdal_tap.patch
    * A discussion thread related to it: https://lists.osgeo.org/pipermail/gdal-dev/2010-October/thread.html#26209
    *
    */
  val alignTargetPixels: Boolean
  /** options from previous transformation steps */
  val baseWarpList: List[GDALWarpOptions]
  /** current transformation options */
  val warpOptions: GDALWarpOptions
  /** the list of transformation options including the current one */
  lazy val warpList: List[GDALWarpOptions] = baseWarpList :+ warpOptions

  // generate a vrt before the current options application
  @transient lazy val fromBaseWarpList: Dataset = GDAL.fromGDALWarpOptions(uri, baseWarpList)
  // generate a vrt with the current options application
  @transient lazy val fromWarpList: Dataset = GDAL.fromGDALWarpOptions(uri, warpList)

  // current dataset
  @transient lazy val dataset: Dataset = fromWarpList

  protected lazy val geoTransform: Array[Double] = dataset.GetGeoTransform

  lazy val bandCount: Int = dataset.getRasterCount

  lazy val crs: CRS =
    Option(dataset.GetProjectionRef)
      .filter(_.nonEmpty)
      .map(new SpatialReference(_).toCRS)
      .getOrElse(CRS.fromEpsgCode(4326))

  private lazy val reader: GDALReader = GDALReader(dataset)

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = {
    val baseBand = fromBaseWarpList.GetRasterBand(1)

    val arr = Array.ofDim[java.lang.Double](1)
    baseBand.GetNoDataValue(arr)
    arr.headOption.flatMap(Option(_)).map(_.doubleValue())
  }

  lazy val cellType: CellType = {
    val (noDataValue, bufferType, typeSizeInBits) = {
      val baseBand = dataset.GetRasterBand(1)

      val arr = Array.ofDim[java.lang.Double](1)
      baseBand.GetNoDataValue(arr)

      val nd = arr.headOption.flatMap(Option(_)).map(_.doubleValue())
      val bufferType = baseBand.getDataType
      val typeSizeInBits = gdal.GetDataTypeSize(bufferType)
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

  lazy val overviewsRasterExtents: List[RasterExtent] = {
    val band = dataset.GetRasterBand(1)
    (0 until band.GetOverviewCount()).toList.map { idx =>
      val ovr = band.GetOverview(idx)
      RasterExtent(extent, CellSize(ovr.GetXSize(), ovr.GetYSize()))
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

  def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, options, alignTargetPixels = alignTargetPixels)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, alignTargetPixels = alignTargetPixels)

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

  override def close = dataset.delete()
}
