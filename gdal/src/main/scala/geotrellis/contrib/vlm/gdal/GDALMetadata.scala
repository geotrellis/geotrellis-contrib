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

import com.azavea.gdal.GDALWarp
import geotrellis.contrib.vlm.SourceMetadata

case class GDALMetadata(
  gdalBaseMetadata: Map[String, Map[String, String]],
  gdalBandsMetadata: List[Map[String, Map[String, String]]]
) extends SourceMetadata {
  lazy val baseMetadata: Map[String, String] = gdalBaseMetadata.flatMap(_._2)
  lazy val bandsMetadata: List[Map[String, String]] = gdalBandsMetadata.map(_.flatMap(_._2))

  def base: Map[String, String] = baseMetadata
  def band(b: Int): Map[String, String] = if(b == 0) baseMetadata else bandsMetadata.lift(b).getOrElse(Map())
}

object GDALMetadata {
  /** https://github.com/geosolutions-it/imageio-ext/blob/1.3.2/library/gdalframework/src/main/java/it/geosolutions/imageio/gdalframework/GDALUtilities.java#L68 */
  object Domain {
    val DEFAULT         = ""
    /** https://github.com/OSGeo/gdal/blob/bed760bfc8479348bc263d790730ef7f96b7d332/gdal/doc/source/development/rfc/rfc14_imagestructure.rst **/
    val IMAGE_STRUCTURE = "IMAGE_STRUCTURE"
    /** https://github.com/OSGeo/gdal/blob/6417552c7b3ef874f8306f83e798f979eb37b309/gdal/doc/source/drivers/raster/eedai.rst#subdatasets */
    val SUBDATASETS     = "SUBDATASETS"

    val ALL = List(DEFAULT, IMAGE_STRUCTURE, SUBDATASETS)
  }

  def apply(dataset: GDALDataset, datasetType: Int = GDALWarp.SOURCE, domains: List[String] = Nil): GDALMetadata = {
    if(domains.isEmpty)
      GDALMetadata(
        dataset.getAllMetadata(datasetType, 0),
        (1 until dataset.bandCount).toList.map(dataset.getAllMetadata(datasetType, _))
      )
    else
      GDALMetadata(
        dataset.getMetadata(datasetType, domains, 0),
        (1 until dataset.bandCount).toList.map(dataset.getMetadata(datasetType, domains, _))
      )
  }
}
