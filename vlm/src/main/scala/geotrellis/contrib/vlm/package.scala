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

import geotrellis.util.{FileRangeReader, RangeReader, StreamingByteReader}
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster.{GridExtent, RasterExtent}
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.reproject.ReprojectRasterExtent
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.s3.AmazonS3Client
import org.apache.http.client.utils.URLEncodedUtils
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import java.nio.file.Paths
import java.net.{URI, URL}
import java.nio.charset.Charset

package object vlm {
  private[vlm] def getByteReader(uri: String): StreamingByteReader =
    new StreamingByteReader(getRangeReader(uri))

  private[vlm] def getRangeReader(uri: String): RangeReader = {
    val javaURI = new URI(uri)
    val noQueryParams = URLEncodedUtils.parse(uri, Charset.forName("UTF-8")).isEmpty

    javaURI.getScheme match {
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
