/*
 * Copyright 2018 Azavea
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

import geotrellis.proj4.CRS

import org.gdal.osr.SpatialReference
import java.util.Base64

package object gdal extends Serializable {
  implicit class SpatialReferenceMethods(val sr: SpatialReference) extends AnyVal {
    def toCRS: CRS = CRS.fromString(sr.ExportToProj4)
  }

  implicit class StringMethods(str: String) {
    def base64: String = Base64.getEncoder.encodeToString(str.getBytes)
  }

  implicit class GDALWarpOptionsListMethods(options: List[GDALWarpOptions]) {
    val name: String = options.map(_.name).mkString("_")
  }

  implicit class GDALWarpOptionsListDependentMethods(options: (GDALWarpOptions, List[GDALWarpOptions])) {
    val name: String = { val (opt, list) = options; s"${opt.name}${list.name}" }
  }

  implicit class GDALWarpOptionsListDatasetDependentMethods(options: (String, List[GDALWarpOptions])) {
    val name: String = { val (path, list) = options; s"${path}${list.name}" }
  }

  implicit class GDALWarpOptionOptionsListDatasetDependentMethods(options: Option[(String, List[GDALWarpOptions])]) {
    val name: String = options.map(_.name).getOrElse("")
  }
}
