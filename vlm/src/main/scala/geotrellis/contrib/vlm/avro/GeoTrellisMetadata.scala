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
import geotrellis.contrib.vlm.{RasterMetadata, RasterSourceMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}

import cats.Monad
import cats.syntax.apply._

case class GeoTrellisMetadata(
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]],
  attributes: Map[String, String]
) extends RasterSourceMetadata {
  /** GeoTrellis metadata doesn't allow to query a per band metadata by default. */
  def attributesForBand(band: Int): Map[String, String] = Map.empty
}

object GeoTrellisMetadata {
  def apply(rasterSource: RasterMetadata, base: Map[String, String]): GeoTrellisMetadata =
    GeoTrellisMetadata(rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions, base)

  def apply[F[_]: Monad](rasterSource: RasterSourceF[F], base: F[Map[String, String]]): F[GeoTrellisMetadata] =
    (rasterSource: F[RasterMetadata], base).mapN(GeoTrellisMetadata.apply)
}
