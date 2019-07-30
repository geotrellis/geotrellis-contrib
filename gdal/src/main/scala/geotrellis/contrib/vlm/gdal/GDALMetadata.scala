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

import geotrellis.contrib.vlm.{RasterSource, RasterSourceMetadata, SourceMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}
import cats.Monad
import cats.syntax.apply._
import cats.syntax.functor._
import geotrellis.contrib.vlm.effect.RasterSourceF

case class GDALMetadata(
  gdalBaseMetadata: Map[String, Map[String, String]],
  gdalBandsMetadata: List[Map[String, Map[String, String]]],
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]]
) extends SourceMetadata {
  lazy val baseMetadata: Map[String, String] = gdalBaseMetadata.flatMap(_._2)
  lazy val bandsMetadata: List[Map[String, String]] = gdalBandsMetadata.map(_.flatMap(_._2))

  def sourceMetadata(): Map[String, String] = baseMetadata
  def sourceMetadata(b: Int): Map[String, String] = if(b == 0) baseMetadata else bandsMetadata.lift(b).getOrElse(Map())
}

object GDALMetadata {

  /** https://github.com/geosolutions-it/imageio-ext/blob/1.3.2/library/gdalframework/src/main/java/it/geosolutions/imageio/gdalframework/GDALUtilities.java#L68 */
  object Domain {
    val DEFAULT = ""
    /** https://github.com/OSGeo/gdal/blob/bed760bfc8479348bc263d790730ef7f96b7d332/gdal/doc/source/development/rfc/rfc14_imagestructure.rst **/
    val IMAGE_STRUCTURE = "IMAGE_STRUCTURE"
    /** https://github.com/OSGeo/gdal/blob/6417552c7b3ef874f8306f83e798f979eb37b309/gdal/doc/source/drivers/raster/eedai.rst#subdatasets */
    val SUBDATASETS = "SUBDATASETS"

    val ALL = List(DEFAULT, IMAGE_STRUCTURE, SUBDATASETS)
  }

  def apply(dataset: GDALDataset, rasterSource: RasterSourceMetadata, datasetType: Int, domains: List[String]): GDALMetadata = {
    if (domains.isEmpty)
      GDALMetadata(
        dataset.getAllMetadata(datasetType, 0),
        (1 until dataset.bandCount).toList.map(dataset.getAllMetadata(datasetType, _)),
        rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions
      )
    else
      GDALMetadata(
        dataset.getMetadata(datasetType, domains, 0),
        (1 until dataset.bandCount).toList.map(dataset.getMetadata(datasetType, domains, _)),
        rasterSource.crs, rasterSource.bandCount, rasterSource.cellType, rasterSource.gridExtent, rasterSource.resolutions
      )
  }

  def apply[F[_] : Monad](dataset: F[GDALDataset], rasterSource: RasterSourceF[F], datasetType: Int, domains: List[String]): F[GDALMetadata] = {
    (dataset, rasterSource: F[RasterSourceMetadata]).mapN(GDALMetadata.apply(_, _, datasetType, domains))
  }
}
