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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.effect.RasterSourceMetadataF
import geotrellis.contrib.vlm.{RasterMetadata, RasterSourceMetadata, SourceName}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}
import geotrellis.raster.io.geotiff.Tags

import cats.Monad
import cats.syntax.apply._

case class GeoTiffMetadata(
  name: SourceName,
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]],
  tags: Tags
) extends RasterSourceMetadata {
  /** Returns the GeoTiff head tags. */
  def attributes: Map[String, String] = tags.headTags
  /** Returns the GeoTiff per band tags. */
  def attributesForBand(band: Int): Map[String, String] = tags.bandTags.lift(band).getOrElse(Map.empty)
}

object GeoTiffMetadata {
  def apply(rasterSource: RasterMetadata, tags: Tags): GeoTiffMetadata =
    GeoTiffMetadata(rasterSource.name, rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions, tags)

  def apply[F[_]: Monad](rasterSource: RasterSourceMetadataF[F], tags: F[Tags]): F[GeoTiffMetadata] =
    (rasterSource: F[RasterMetadata], tags).mapN(GeoTiffMetadata.apply)
}
