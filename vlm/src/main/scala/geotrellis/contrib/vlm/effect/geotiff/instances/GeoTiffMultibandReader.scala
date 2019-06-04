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

package geotrellis.contrib.vlm.effect.geotiff.instances

import geotrellis.raster.io.geotiff._
import cats.effect.IO

trait GeoTiffMultibandReader[F[_]] {
  def read(uri: String): F[MultibandGeoTiff]
}

object GeoTiffMultibandReader {
  def apply[F[_]: GeoTiffMultibandReader]: GeoTiffMultibandReader[F] = implicitly[GeoTiffMultibandReader[F]]

  implicit val geoTiffMultibandReaderIO: GeoTiffMultibandReaderSync[IO] = new GeoTiffMultibandReaderSync[IO]
  implicit val geoTiffMultibandReaderId: GeoTiffMultibandReaderOption = new GeoTiffMultibandReaderOption
}
