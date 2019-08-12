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

import geotrellis.contrib.vlm.{RasterMetadata, SourceName}
import geotrellis.contrib.vlm.effect.RasterMetadataF
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}

import cats.Monad
import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._

case class GDALMetadata(
  name: SourceName,
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]],
  /** GDAL per domain metadata */
  baseMetadata: Map[GDALMetadataDomain, Map[String, String]] = Map.empty,
  /** GDAL per band per domain metadata */
  bandsMetadata: List[Map[GDALMetadataDomain, Map[String, String]]] = Nil
) extends RasterMetadata {
  /** Returns the GDAL metadata merged into a single metadata domain. */
  def attributes: Map[String, String] = baseMetadata.flatMap(_._2)
  /** Returns the GDAL per band metadata merged into a single metadata domain. */
  def attributesForBand(band: Int): Map[String, String] = bandsMetadata.map(_.flatMap(_._2)).lift(band).getOrElse(Map.empty)
}

object GDALMetadata {
  def apply(rasterMetadata: RasterMetadata, dataset: GDALDataset, domains: List[GDALMetadataDomain]): GDALMetadata =
    domains match {
      case Nil =>
        GDALMetadata(rasterMetadata.name, rasterMetadata.crs, rasterMetadata.bandCount, rasterMetadata.cellType, rasterMetadata.gridExtent, rasterMetadata.resolutions)
      case _ =>
        GDALMetadata(
          rasterMetadata.name, rasterMetadata.crs, rasterMetadata.bandCount, rasterMetadata.cellType, rasterMetadata.gridExtent, rasterMetadata.resolutions,
          dataset.getMetadata(GDALDataset.SOURCE, domains, 0),
          (1 until dataset.bandCount).toList.map(dataset.getMetadata(GDALDataset.SOURCE, domains, _))
        )
    }

  def apply(rasterMetadata: RasterMetadata, dataset: GDALDataset): GDALMetadata =
    GDALMetadata(
      rasterMetadata.name, rasterMetadata.crs, rasterMetadata.bandCount, rasterMetadata.cellType, rasterMetadata.gridExtent, rasterMetadata.resolutions,
      dataset.getAllMetadata(GDALDataset.SOURCE, 0),
      (1 until dataset.bandCount).toList.map(dataset.getAllMetadata(GDALDataset.SOURCE, _))
    )

  def apply[F[_] : Monad](rasterMetadata: RasterMetadataF[F], dataset: F[GDALDataset], domains: List[GDALMetadataDomain]): F[GDALMetadata] =
    domains match {
      case Nil =>
        (Monad[F].pure(rasterMetadata.name), rasterMetadata.crs, rasterMetadata.bandCount, rasterMetadata.cellType, rasterMetadata.gridExtent, rasterMetadata.resolutions).mapN(GDALMetadata(_, _, _, _, _, _))
      case _ =>
        (Monad[F].pure(rasterMetadata.name),
          rasterMetadata.crs,
          rasterMetadata.bandCount,
          rasterMetadata.cellType,
          rasterMetadata.gridExtent,
          rasterMetadata.resolutions,
          dataset.map(_.getAllMetadata(GDALDataset.SOURCE, 0)),
          dataset >>= { ds =>
            (1 until ds.bandCount).toList.traverse { list =>
              dataset.map(_.getMetadata(GDALDataset.SOURCE, domains, list))
            }
          }).mapN(apply)
    }

  def apply[F[_] : Monad](rasterMetadata: RasterMetadataF[F], dataset: F[GDALDataset]): F[GDALMetadata] =
    (Monad[F].pure(rasterMetadata.name),
     rasterMetadata.crs,
     rasterMetadata.bandCount,
     rasterMetadata.cellType,
     rasterMetadata.gridExtent,
     rasterMetadata.resolutions,
     dataset.map(_.getAllMetadata(GDALDataset.SOURCE, 0)),
     dataset >>= { ds =>
       (1 until ds.bandCount).toList.traverse { list =>
         dataset.map(_.getAllMetadata(GDALDataset.SOURCE, list))
       }
     }).mapN(apply)
}
