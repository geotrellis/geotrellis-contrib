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

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File

import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource

object TestCatalog {
  val filePath = s"${new File("").getAbsolutePath()}/src/test/resources/img/aspect-tiled.tif"
  val inputPath = s"file://$filePath"
  val outputPath = s"${new File("").getAbsolutePath()}/src/test/resources/data/catalog"

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  def create(implicit sc: SparkContext) = {
    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    val rs = GeoTiffRasterSource(TestCatalog.filePath)
    rs.resolutions.sortBy(_.cellSize.resolution).zipWithIndex.foreach { case (rasterExtent, index) =>
      val layout = LayoutDefinition(rasterExtent, tileSize = 256)

      val rdd: MultibandTileLayerRDD[SpatialKey] =
        RasterSourceRDD(List(rs), layout)
          .withContext( tiledd =>
            // the tiles are actually `PaddedTile`, this forces them to be ArrayTile
            tiledd.mapValues { mb: MultibandTile => ArrayMultibandTile(mb.bands.map(_.toArrayTile))}
          )

      val id = LayerId("landsat", index)
      writer.write(id, rdd, ZCurveKeyIndexMethod)
    }
  }
}
