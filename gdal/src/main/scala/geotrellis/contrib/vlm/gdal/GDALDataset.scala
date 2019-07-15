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

import geotrellis.contrib.vlm.gdal.config.GDALOptionsConfig

import geotrellis.raster._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.Extent
import com.azavea.gdal.GDALWarp

case class GDALDataset(token: Long) extends AnyVal {
  def getProjection: Option[String] = getProjection(GDALWarp.WARPED)

  def getProjection(dataset: Int): Option[String] = {
    require(acceptableDatasets contains dataset)
    val crs = Array.ofDim[Byte](1 << 16)
    if (GDALWarp.get_crs_wkt(token, dataset, numberOfAttempts, crs) <= 0)
      throw new MalformedProjectionException("Unable to parse projection as WKT String")
    Some(new String(crs, "UTF-8"))
  }

  def rasterExtent: RasterExtent = rasterExtent(GDALWarp.WARPED)

  def rasterExtent(dataset: Int): RasterExtent = {
    require(acceptableDatasets contains dataset)
    val transform = Array.ofDim[Double](6)
    val width_height = Array.ofDim[Int](2)
    if (GDALWarp.get_transform(token, dataset, numberOfAttempts, transform) <= 0 ||
      GDALWarp.get_width_height(token, dataset, numberOfAttempts, width_height) <= 0)
      throw new MalformedDataException("Bad tranform values read")

    val x1 = transform(0)
    val y1 = transform(3)
    val x2 = x1 + transform(1) * width_height(0)
    val y2 = y1 + transform(5) * width_height(1)
    val e = Extent(
      math.min(x1, x2),
      math.min(y1, y2),
      math.max(x1, x2),
      math.max(y1, y2)
    )

    RasterExtent(e, math.abs(transform(1)), math.abs(transform(5)), width_height(0), width_height(1))
  }

  def resolutions(): List[RasterExtent] = resolutions(GDALWarp.WARPED)

  def resolutions(dataset: Int): List[RasterExtent] = {
    require(acceptableDatasets contains dataset)
    val N = 1 << 8
    val widths = Array.ofDim[Int](N)
    val heights = Array.ofDim[Int](N)
    val extent = this.extent(dataset)

    if (GDALWarp.get_overview_widths_heights(token, dataset, numberOfAttempts, 1, widths, heights) <= 0)
      throw new MalformedDataException("Unable to read in overviews")
    widths.zip(heights).flatMap({ case (w, h) =>
      if (w > 0 && h > 0) Some(RasterExtent(extent, cols = w, rows = h))
      else None
    }).toList
  }

  def extent: Extent = extent(GDALWarp.WARPED)

  def extent(dataset: Int): Extent = {
    require(acceptableDatasets contains dataset)
    this.rasterExtent(dataset).extent
  }

  def bandCount: Int = bandCount(GDALWarp.WARPED)

  def bandCount(dataset: Int): Int = {
    require(acceptableDatasets contains dataset)
    val count = Array.ofDim[Int](1)
    if (GDALWarp.get_band_count(token, dataset, numberOfAttempts, count) <= 0)
      throw new MalformedDataException("A bandCount of <= 0 was found")
    count(0)
  }

  def crs: CRS = crs(GDALWarp.WARPED)

  def crs(dataset: Int): CRS = {
    require(acceptableDatasets contains dataset)
    val crs = Array.ofDim[Byte](1 << 16)
    if (GDALWarp.get_crs_proj4(token, dataset, numberOfAttempts, crs) <= 0)
      throw new MalformedProjectionException("Unable to parse projection as CRS")
    val proj4String: String = new String(crs, "UTF-8").trim
    if (proj4String.length > 0) CRS.fromString(proj4String.trim)
    else LatLng
  }

  def noDataValue: Option[Double] = noDataValue(GDALWarp.WARPED)

  def noDataValue(dataset: Int): Option[Double] = {
    require(acceptableDatasets contains dataset)
    val nodata = Array.ofDim[Double](1)
    val success = Array.ofDim[Int](1)
    if (GDALWarp.get_band_nodata(token, dataset, numberOfAttempts, 1, nodata, success) <= 0)
      throw new MalformedDataTypeException("Unable to determine NoData value")
    if (success(0) == 0) None
    else Some(nodata(0))
  }

  def dataType: Int = dataType(GDALWarp.WARPED)

  def dataType(dataset: Int): Int = {
    require(acceptableDatasets contains dataset)
    val dataType = Array.ofDim[Int](1)
    if (GDALWarp.get_band_data_type(token, dataset, numberOfAttempts, 1, dataType) <= 0)
      throw new MalformedDataTypeException("Unknown DataType")
    dataType(0)
  }

  def cellSize: CellSize = cellSize(GDALWarp.WARPED)

  def cellSize(dataset: Int): CellSize = {
    require(acceptableDatasets contains dataset)
    val transform = Array.ofDim[Double](6)
    GDALWarp.get_transform(token, dataset, numberOfAttempts, transform)
    CellSize(transform(1), transform(5))
  }

  def cellType: CellType = cellType(GDALWarp.WARPED)

  def cellType(dataset: Int): CellType = {
    require(acceptableDatasets contains dataset)
    val nd = noDataValue(dataset)
    val dt = GDALDataType.intToGDALDataType(this.dataType(dataset))
    val mm = {
      val minmax = Array.ofDim[Double](2)
      val success = Array.ofDim[Int](1)
      if (GDALWarp.get_band_min_max(token, dataset, numberOfAttempts, 1, true, minmax, success) <= 0)
        throw new MalformedDataTypeException("Bad min/max values")
      if (success(0) != 0) Some(minmax(0), minmax(1))
      else None
    }
    GDALUtils.dataTypeToCellType(datatype = dt, noDataValue = nd, minMaxValues = mm)
  }

  def readTile(gb: GridBounds[Int] = rasterExtent.gridBounds, band: Int, dataset: Int = GDALWarp.WARPED): Tile = {
    require(acceptableDatasets contains dataset)
    val GridBounds(xmin, ymin, xmax, ymax) = gb
    val srcWindow: Array[Int] = Array(xmin, ymin, xmax - xmin + 1, ymax - ymin + 1)
    val dstWindow: Array[Int] = Array(srcWindow(2), srcWindow(3))
    val ct = this.cellType(dataset)
    val dt = this.dataType(dataset)
    val bytes = Array.ofDim[Byte](dstWindow(0) * dstWindow(1) * ct.bytes)

    if (GDALWarp.get_data(token, dataset, numberOfAttempts, srcWindow, dstWindow, band, dt, bytes) <= 0)
      throw new GDALIOException("Unable to read in data")
    ArrayTile.fromBytes(bytes, ct, dstWindow(0), dstWindow(1))
  }

  def readMultibandTile(gb: GridBounds[Int] = rasterExtent.gridBounds, bands: Seq[Int] = 1 to bandCount, dataset: Int = GDALWarp.WARPED): MultibandTile =
    MultibandTile(bands.map { readTile(gb, _, dataset) })

  def readMultibandRaster(gb: GridBounds[Int] = rasterExtent.gridBounds, bands: Seq[Int] = 1 to bandCount, dataset: Int = GDALWarp.WARPED): Raster[MultibandTile] =
    Raster(readMultibandTile(gb, bands, dataset), rasterExtent.rasterExtentFor(gb).extent)
}

object GDALDataset {
  GDALOptionsConfig.setOptions
  def apply(uri: String, options: Array[String]): GDALDataset = GDALDataset(GDALWarp.get_token(uri, options))
  def apply(uri: String): GDALDataset = apply(uri, Array())
}
