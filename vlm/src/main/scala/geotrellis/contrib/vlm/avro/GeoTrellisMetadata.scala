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

import geotrellis.contrib.vlm.effect.RasterSourceF
import geotrellis.contrib.vlm.{RasterSourceMetadata, SourceMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}

import cats.Monad
import cats.syntax.apply._

case class GeoTrellisMetadata(
  sourceMetadata: Map[String, String],
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]]
) extends SourceMetadata {
  def sourceMetadata(b: Int): Map[String, String] = sourceMetadata
}

object GeoTrellisMetadata {
  def apply(base: Map[String, String], rasterSource: RasterSourceMetadata): GeoTrellisMetadata =
    GeoTrellisMetadata(base, rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions)

  def apply[F[_]: Monad](base: F[Map[String, String]], rasterSource: RasterSourceF[F]): F[GeoTrellisMetadata] =
    (base, rasterSource: F[RasterSourceMetadata]).mapN(GeoTrellisMetadata.apply)
}
