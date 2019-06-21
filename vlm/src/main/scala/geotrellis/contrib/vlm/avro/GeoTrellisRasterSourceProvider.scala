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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.spark.LayerId


class GeoTrellisRasterSourceProvider extends RasterSourceProvider {
  def canProcess(path: String): Boolean =
    try {
      GeoTrellisDataPath(path)
      return true
    } catch {
      case _: Throwable => false
    }

    def rasterSource(path: String): GeotrellisRasterSource = {
      val dataPath = GeoTrellisDataPath(path)
      val layerName = dataPath.layerName

      // TODO: Handle the case where there's no zoom level
      val zoomLevel = dataPath.zoomLevel.get
      val layerId = LayerId(layerName, zoomLevel)

      dataPath.bandCount match {
        case Some(bandCount) => new GeotrellisRasterSource(dataPath, layerId, bandCount)
        case None => new GeotrellisRasterSource(dataPath, layerId)
      }
    }
}
