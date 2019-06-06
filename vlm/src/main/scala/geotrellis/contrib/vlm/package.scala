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

package geotrellis.contrib

import geotrellis.contrib.vlm.config.{S3Config, HdfsConfig}
import geotrellis.util.{FileRangeReader, StreamingByteReader}
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.spark.io.s3.AmazonS3Client

import org.apache.http.client.utils.URLEncodedUtils
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.nio.file.Paths
import java.net.{URI, URL}
import java.nio.charset.Charset

package object vlm extends vlm.Implicits {
  private[vlm] def getByteReader(uri: String): StreamingByteReader = {
    val javaURI = new URI(uri)
    val noQueryParams = URLEncodedUtils.parse(uri, Charset.forName("UTF-8")).isEmpty

    val rr =  javaURI.getScheme match {
      case null =>
        FileRangeReader(Paths.get(uri).toFile)

      case "file" =>
        FileRangeReader(Paths.get(javaURI).toFile)

      case "http" | "https" if noQueryParams =>
        HttpRangeReader(new URL(uri))

      case "http" | "https" =>
        new HttpRangeReader(new URL(uri), false)

      case "hdfs" =>
        new HdfsRangeReader(new Path(uri), HdfsConfig.load())

      case "s3" =>
        val s3Uri = new AmazonS3URI(java.net.URLDecoder.decode(uri, "UTF-8"))
        val s3Client = if (S3Config.allowGlobalRead) {
          val builder = AmazonS3ClientBuilder
            .standard()
            .withForceGlobalBucketAccessEnabled(true)

          val client = S3Config.region.fold(builder) { region => builder.setRegion(region); builder }.build

          new AmazonS3Client(client)
        } else {
          val builder = AmazonS3ClientBuilder.standard()
          val client = S3Config.region.fold(builder) { region => builder.setRegion(region); builder }.build
          new AmazonS3Client(client)
        }
        S3RangeReader(s3Uri.getBucket, s3Uri.getKey, s3Client)

      case scheme =>
        throw new IllegalArgumentException(s"Unable to read scheme $scheme at $uri")
    }
    new StreamingByteReader(rr)
  }
}
