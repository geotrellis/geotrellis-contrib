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

package geotrellis.contrib.vlm.gdal.effect

import geotrellis.raster.gdal._
import geotrellis.contrib.vlm.effect.RasterMetadataF

import cats.Monad
import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._

object GDALMetadataF {
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
          }).mapN(GDALMetadata.apply)
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
      }).mapN(GDALMetadata.apply)
}
