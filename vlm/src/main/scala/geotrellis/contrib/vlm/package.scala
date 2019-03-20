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

package geotrellis.contrib

import geotrellis.util.{FileRangeReader, StreamingByteReader}
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster.{GridExtent, RasterExtent}
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.s3.AmazonS3Client
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.contrib.vlm.gdal.{GDALUtils, GDALDataType}

import org.apache.http.client.utils.URLEncodedUtils
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}

import java.nio.file.Paths
import java.net.{URI, URL}
import java.nio.charset.Charset

import com.azavea.gdal.GDALWarp


package object vlm {
  val acceptableDatasets = Set(GDALWarp.SOURCE, GDALWarp.WARPED)
  val numberOfAttempts = 1 << 20

  private[vlm] def getByteReader(uri: String): StreamingByteReader = {
    val javaURI = new URI(uri)
    val noQueryParams = URLEncodedUtils.parse(uri, Charset.forName("UTF-8")).isEmpty

    val rr =  javaURI.getScheme match {
      case null =>
        FileRangeReader(Paths.get(javaURI.toString).toFile)

      case "file" =>
        FileRangeReader(Paths.get(javaURI).toFile)

      case "http" | "https" if noQueryParams =>
        HttpRangeReader(new URL(uri))

      case "http" | "https" =>
        new HttpRangeReader(new URL(uri), false)

      case "s3" =>
        val s3Uri = new AmazonS3URI(java.net.URLDecoder.decode(uri, "UTF-8"))
        val s3Client = if (Config.s3.allowGlobalRead) {
          new AmazonS3Client(
            AmazonS3ClientBuilder
              .standard()
              .withForceGlobalBucketAccessEnabled(true)
              .build()
          )
        } else {
          new AmazonS3Client(AmazonS3ClientBuilder.defaultClient())
        }
        S3RangeReader(s3Uri.getBucket, s3Uri.getKey, s3Client)

      case scheme =>
        throw new IllegalArgumentException(s"Unable to read scheme $scheme at $uri")
    }
    new StreamingByteReader(rr)
  }

  implicit class tokenMethods(val token: Long) extends AnyVal {

    def getProjection: Option[String] = getProjection(GDALWarp.WARPED)

    def getProjection(dataset: Int): Option[String] = {
      require(acceptableDatasets contains dataset)
      val crs = Array.ofDim[Byte](1 << 16)
      if (GDALWarp.get_crs_wkt(token, dataset, numberOfAttempts, crs) <= 0)
        throw new Exception("get_crs_wkt")
      Some(new String(crs, "UTF-8"))
    }

    def rasterExtent: RasterExtent = rasterExtent(GDALWarp.WARPED)

    def rasterExtent(dataset: Int): RasterExtent = {
      require(acceptableDatasets contains dataset)
      val transform = Array.ofDim[Double](6)
      val width_height = Array.ofDim[Int](2)
      if (GDALWarp.get_transform(token, dataset, numberOfAttempts, transform) <= 0 ||
          GDALWarp.get_width_height(token, dataset, numberOfAttempts, width_height) <= 0)
        throw new Exception("get_transform or get_widht_height")

      val x1 = transform(0)
      val y1 = transform(3)
      val x2 = x1 + transform(1) * width_height(0)
      val y2 = y1 + transform(5) * width_height(1)
      val e = Extent(
        math.min(x1, x2),
        math.min(y1, y2),
        math.max(x1, x2),
        math.max(y1, y2))

      RasterExtent(e,
        math.abs(transform(1)), math.abs(transform(5)),
        width_height(0), width_height(1))
    }

    def resolutions: List[RasterExtent] = resolutions(GDALWarp.WARPED)

    def resolutions(dataset: Int): List[RasterExtent] = {
      require(acceptableDatasets contains dataset)
      val N = 1 << 8;
      val widths = Array.ofDim[Int](N)
      val heights = Array.ofDim[Int](N)
      val extent = token.extent(dataset)

      if (GDALWarp.get_overview_widths_heights(token, dataset, numberOfAttempts, widths, heights) <= 0)
        throw new Exception("get_overview_widths_heights")
      widths.zip(heights).flatMap({ case (w, h) =>
        if (w > 0 && h > 0) Some(RasterExtent(extent, cols = w, rows = h))
        else None
      }).toList
    }

    def extent: Extent = extent(GDALWarp.WARPED)

    def extent(dataset: Int): Extent = {
      require(acceptableDatasets contains dataset)
      token.rasterExtent(dataset).extent
    }

    def bandCount: Int = bandCount(GDALWarp.WARPED)

    def bandCount(dataset: Int): Int = {
      require(acceptableDatasets contains dataset)
      val count = Array.ofDim[Int](1)
      if (GDALWarp.get_band_count(token, dataset, numberOfAttempts, count) <= 0)
        throw new Exception("get_band_count")
      count(0)
    }

    def crs: CRS = crs(GDALWarp.WARPED)

    def crs(dataset: Int): CRS = {
      require(acceptableDatasets contains dataset)
      val crs = Array.ofDim[Byte](1 << 16)
      if (GDALWarp.get_crs_proj4(token, dataset, numberOfAttempts, crs) <= 0)
        throw new Exception("get_crs_proj4")
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
        throw new Exception("get_band_nodata")
      if (success(0) == 0)
        None
      else
        Some(nodata(0))
    }

    def dataType: Int = dataType(GDALWarp.WARPED)

    def dataType(dataset: Int): Int = {
      require(acceptableDatasets contains dataset)
      val dataType = Array.ofDim[Int](1)
      if (GDALWarp.get_band_data_type(token, dataset, numberOfAttempts, 1, dataType) <= 0)
        throw new Exception("get_band_data_type")
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
      val dt = GDALDataType.intToGDALDataType(token.dataType(dataset))
      val mm = {
        val minmax = Array.ofDim[Double](2)
        val success = Array.ofDim[Int](1)
        if (GDALWarp.get_band_min_max(token, dataset, numberOfAttempts, 1, true, minmax, success) <= 0)
          throw new Exception("get_band_min_max")
        if (success(0) != 0) Some(minmax(0), minmax(1))
        else None
      }
      GDALUtils.dataTypeToCellType(datatype = dt, noDataValue = nd, minMaxValues = mm)
    }

    def readTile(gb: GridBounds, band: Int, dataset: Int = GDALWarp.WARPED): Tile = {
      require(acceptableDatasets contains dataset)
      val GridBounds(xmin, ymin, xmax, ymax) = gb
      val srcWindow: Array[Int] = Array(xmin, ymin, xmax - xmin + 1, ymax - ymin + 1)
      val dstWindow: Array[Int] = Array(srcWindow(2), srcWindow(3))
      val ct = token.cellType(dataset)
      val dt = token.dataType(dataset)
      val bytes = Array.ofDim[Byte](dstWindow(0) * dstWindow(1) * ct.bytes)

      if (GDALWarp.get_data(token, dataset, numberOfAttempts, srcWindow, dstWindow, band, dt, bytes) <= 0)
        throw new Exception("get_data")
      ArrayTile.fromBytes(bytes, ct, dstWindow(0), dstWindow(1))
    }

  }

  implicit class rasterExtentMethods(self: RasterExtent) {
    def reproject(src: CRS, dest: CRS, options: Options): RasterExtent =
      if(src == dest) self
      else {
        val transform = Transform(src, dest)
        options.targetRasterExtent.getOrElse(ReprojectRasterExtent(self, transform, options = options))
      }

    def reproject(src: CRS, dest: CRS): RasterExtent =
      reproject(src, dest, Options.DEFAULT)

    def toGridExtent: GridExtent = GridExtent(self.extent, self.cellheight, self.cellwidth)
  }
}
