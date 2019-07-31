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

import geotrellis.contrib.vlm.{RasterMetadata, RasterSourceMetadata}
import geotrellis.contrib.vlm.effect.RasterSourceF
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}
import cats.Monad
import cats.syntax.apply._
import com.azavea.gdal.GDALWarp

case class GDALMetadata(
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]],
  /** GDAL per domain metadata */
  baseMetadata: Map[GDALMetadataDomain, Map[String, String]] = Map.empty,
  /** GDAL per band per domain metadata */
  bandsMetadata: List[Map[GDALMetadataDomain, Map[String, String]]] = Nil
) extends RasterSourceMetadata {
  /** Returns the GDAL metadata merged into a single metadata domain. */
  def attributes: Map[String, String] = baseMetadata.flatMap(_._2)
  /** Returns the GDAL per band metadata merged into a single metadata domain. */
  def attributesForBand(band: Int): Map[String, String] = bandsMetadata.map(_.flatMap(_._2)).lift(band).getOrElse(Map())
}

object GDALMetadata {
  def apply(rasterSource: RasterMetadata, dataset: GDALDataset, domains: GDALMetadataDomainList): GDALMetadata = {
    domains match {
      case DomainList(domains) if domains.isEmpty =>
        GDALMetadata(rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions)
      case DomainList(domains) =>
        GDALMetadata(
          rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions,
          dataset.getMetadata(GDALWarp.SOURCE, domains, 0),
          (1 until dataset.bandCount).toList.map(dataset.getMetadata(GDALWarp.SOURCE, domains, _))
        )
      case FullDomainList =>
        GDALMetadata(
          rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions,
          dataset.getAllMetadata(GDALWarp.SOURCE, 0),
          (1 until dataset.bandCount).toList.map(dataset.getAllMetadata(GDALWarp.SOURCE, _))
        )
    }
  }

  def apply[F[_] : Monad](rasterSource: RasterSourceF[F], dataset: F[GDALDataset], domains: GDALMetadataDomainList): F[GDALMetadata] = {
    (rasterSource: F[RasterMetadata], dataset).mapN(GDALMetadata.apply(_, _, domains))
  }
}
