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

import geotrellis.contrib.vlm.effect.RasterSourceF
import geotrellis.contrib.vlm.{RasterSourceMetadata, SourceMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}
import geotrellis.raster.io.geotiff.Tags

import cats.Monad
import cats.syntax.apply._

case class GeoTiffMetadata(
  tags: Tags,
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]]
) extends SourceMetadata {
  def sourceMetadata(): Map[String, String] = tags.headTags
  def sourceMetadata(b: Int): Map[String, String] = if(b == 0) sourceMetadata() else tags.bandTags.lift(b).getOrElse(Map())
}

object GeoTiffMetadata {
  def apply(tags: Tags, rasterSource: RasterSourceMetadata): GeoTiffMetadata =
    GeoTiffMetadata(tags, rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions)

  def apply[F[_]: Monad](tags: F[Tags], rasterSource: RasterSourceF[F]): F[GeoTiffMetadata] =
    (tags, rasterSource: F[RasterSourceMetadata]).mapN(GeoTiffMetadata.apply)
}
