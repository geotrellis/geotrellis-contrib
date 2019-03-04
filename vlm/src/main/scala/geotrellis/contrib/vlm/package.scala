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
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster.{GridExtent, RasterExtent}
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.s3.AmazonS3Client
import geotrellis.vector._
import geotrellis.raster._

import org.apache.http.client.utils.URLEncodedUtils
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}

import java.nio.file.Paths
import java.net.{URI, URL}
import java.nio.charset.Charset

import com.azavea.gdal.GDALWarp


package object vlm {
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

    def getProjection(): Option[String] = {
      val crs = Array.ofDim[Byte](1 << 16)
      GDALWarp.get_crs_wkt(token, 0, crs)
      Some(new String(crs, "UTF-8"))
    }

    def rasterExtent(): RasterExtent = {
      val transform = Array.ofDim[Double](6)
      val width_height = Array.ofDim[Int](2)
      GDALWarp.get_transform(token, 0, transform)
      GDALWarp.get_width_height(token, 0, width_height)

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

    def resolutions: List[RasterExtent] = {
      val N = 1 << 8;
      val widths = Array.ofDim[Int](N)
      val heights = Array.ofDim[Int](N)
      val extent = token.extent
      GDALWarp.get_overview_widths_heights(token, 0, widths, heights)
      widths.zip(heights).flatMap({ case (w, h) =>
        if (w > 0 && h > 0) Some(RasterExtent(extent, cols = w, rows = h))
        else None
      }).toList
    }

    def extent(): Extent = token.rasterExtent.extent

    def bandCount(): Int = {
      val count = Array.ofDim[Int](1)
      GDALWarp.get_band_count(token, 0, count)
      count(0)
    }

    def crs(): CRS = {
      val crs = Array.ofDim[Byte](1 << 16)
      GDALWarp.get_crs_proj4(token, 0, crs)
      val proj4String: String = new String(crs, "UTF-8")
      CRS.fromString(proj4String.trim)
    }

    def noDataValue(): Option[Double] = {
      val nodata = Array.ofDim[Double](1)
      val success = Array.ofDim[Int](1)
      GDALWarp.get_band_nodata(token, 0, 1, nodata, success)
      if (success(0) == 0)
        None
      else
        Some(nodata(0))
    }

    def dataType(): Int = {
      val dataType = Array.ofDim[Int](1)
      GDALWarp.get_band_data_type(token, 0, 1, dataType)
      dataType(0)
    }

    def cellSize(): CellSize = {
      val transform = Array.ofDim[Double](6)
      GDALWarp.get_transform(token, 0, transform)
      CellSize(transform(1), transform(5))
    }

    def cellType(): CellType = {
      val nd = noDataValue
      token.dataType match {
        case GDALWarp.GDT_Byte => ByteCells.withNoData(nd.map(_.toByte))
        case GDALWarp.GDT_UInt16 => UShortCellType
        case GDALWarp.GDT_Int16 => ShortCells.withNoData(nd.map(_.toShort))
        case GDALWarp.GDT_UInt32 => throw new Exception("Unsupported data type")
        case GDALWarp.GDT_Int32 => IntCells.withNoData(nd.map(_.toInt))
        case GDALWarp.GDT_Float32 => FloatCells.withNoData(nd.map(_.toFloat))
        case GDALWarp.GDT_Float64 => DoubleCells.withNoData(nd)
        case GDALWarp.GDT_CInt16 => throw new Exception("Unsupported data type")
        case GDALWarp.GDT_CInt32 => throw new Exception("Unsupported data type")
        case GDALWarp.GDT_CFloat32 => throw new Exception("Unsupported data type")
        case GDALWarp.GDT_CFloat64 => throw new Exception("Unsupported data type")
        case _ => throw new Exception("Unknown data type")
      }
    }

    def readTile(gb: GridBounds, band: Int): Tile = {
      val xmin = gb.colMin
      val xmax = gb.colMax
      val ymin = gb.rowMin
      val ymax = gb.rowMax
      val srcWindow: Array[Int] = Array(xmin, ymin, xmax - xmin + 1, ymax - ymin + 1)
      val dstWindow: Array[Int] = Array(srcWindow(2), srcWindow(3))
      val bytes = Array.ofDim[Byte](dstWindow(0) * dstWindow(1) * cellType.bytes)

      GDALWarp.get_data(token, 0, srcWindow, dstWindow, band, dataType, bytes)
      ArrayTile.fromBytes(bytes, cellType, dstWindow(0), dstWindow(1))
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
