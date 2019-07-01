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

import geotrellis.contrib.vlm.RasterSourceProvider
import geotrellis.contrib.vlm.avro.GeoTrellisDataPath
import geotrellis.contrib.vlm.geotiff.GeoTiffDataPath

class GDALRasterSourceProvider extends RasterSourceProvider {
  def canProcess(path: String): Boolean =
    (!path.startsWith(GeoTiffDataPath.PREFIX) && !path.startsWith(GeoTrellisDataPath.PREFIX)) && GDALDataPath(path).vsiPath.nonEmpty && path.nonEmpty

  def rasterSource(path: String): GDALRasterSource = GDALRasterSource(path)
}
