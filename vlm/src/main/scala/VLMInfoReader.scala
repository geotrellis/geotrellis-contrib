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

package geotrellis.contrib.vlm

import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.raster.io.geotiff.tags.TiffTags
import geotrellis.spark.io._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import java.net.URI


private[vlm] trait VLMInfoReader extends InfoReader {
  def getGeoTiffInfo(uris: RDD[URI]): RDD[GeoTiffInfo]

  def getGeoTiffTags(uris: RDD[URI]): RDD[TiffTags]
}
