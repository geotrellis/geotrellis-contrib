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

package geotrellis.contrib.vlm

import geotrellis.contrib.vlm.effect.RasterSourceMetadataF
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}

import cats.data.NonEmptyList
import cats.Monad
import cats.syntax.apply._

case class MosaicMetadata(
  name: SourceName,
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]],
  list: NonEmptyList[RasterSourceMetadata]
) extends RasterSourceMetadata {
  /** Mosaic metadata usually doesn't contain a metadata that is common for all RasterSources */
  def attributes: Map[String, String] = Map.empty
  def attributesForBand(band: Int): Map[String, String] = Map.empty
}

object MosaicMetadata {
  def apply(rasterSource: RasterMetadata, list: NonEmptyList[RasterSourceMetadata]): MosaicMetadata =
    MosaicMetadata(rasterSource.name, rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions, list)

  def apply[F[_]: Monad](rasterSource: RasterSourceMetadataF[F], list: F[NonEmptyList[RasterSourceMetadata]]): F[MosaicMetadata] =
    (rasterSource: F[RasterMetadata], list).mapN(MosaicMetadata.apply)
}
