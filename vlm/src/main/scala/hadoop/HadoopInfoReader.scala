/*
 * Copyright 2016 Azavea
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

package geotrellis.contrib.vlm.hadoop

import geotrellis.contrib.vlm._

import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.raster.io.geotiff.tags.TiffTags
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.util.ByteReader

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI


case class HadoopInfoReader(
  config: SerializableConfiguration,
  tiffExtensions: Seq[String] = HadoopGeoTiffRDD.Options.DEFAULT.tiffExtensions,
  streaming: Boolean = true
) extends VLMInfoReader with GTInfoReader {
  def geoTiffInfoRDD(path: URI)(implicit sc: SparkContext): RDD[String] = {
    sc.parallelize(
      HdfsUtils
        .listFiles(new Path(path), config.value)
        .map({ path => path.toString })
        .filter({ path => tiffExtensions.exists({ e => path.endsWith(e) }) })
    )
  }

  def getGeoTiffInfo(uri: URI): GeoTiffInfo = {
    val rr = HdfsRangeReader(new Path(uri), config.value)
    val ovrPath = new Path(s"${uri}.ovr")
    val ovrReader: Option[ByteReader] =
      if (HdfsUtils.pathExists(ovrPath, config.value)) Some(HdfsRangeReader(ovrPath, config.value)) else None

    GeoTiffReader.readGeoTiffInfo(rr, streaming, true, ovrReader)
  }

  def getGeoTiffTags(uri: URI): TiffTags = {
    val rr = HdfsRangeReader(new Path(uri), config.value)
    TiffTags(rr)
  }

  def getGeoTiffInfo(uris: RDD[URI]): RDD[GeoTiffInfo] =
    uris.map { getGeoTiffInfo }

  def getGeoTiffTags(uris: RDD[URI]): RDD[TiffTags] =
    uris.map { getGeoTiffTags }
}
