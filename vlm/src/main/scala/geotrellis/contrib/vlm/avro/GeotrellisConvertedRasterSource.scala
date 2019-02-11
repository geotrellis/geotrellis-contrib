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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.io.geotiff.{Auto, AutoHigherResolution, Base, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.{LayerId, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._
import geotrellis.contrib.vlm.avro._
import geotrellis.raster.{MultibandTile, Tile}

case class GeoTrellisConvertedRasterSource(
  uri: String,
  layerId: LayerId,
  cellType: CellType,
  bandCount: Int,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[avro] val parentOptions: RasterViewOptions = RasterViewOptions()
) extends GeotrellisBaseRasterSource {

  val baseLayerId = layerId

  val resolutions = baseResolutions

  private[avro] val options: RasterViewOptions = parentOptions.copy(cellType = Some(cellType))

  lazy val rasterExtent: RasterExtent = parentRasterExtent

  def crs: CRS = parentCRS
  def resampleMethod: Option[ResampleMethod] = None

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val result = GeotrellisRasterSource.read(reader, layerId, metadata, extent, bands)

    result.map { raster =>
      raster.mapTile { _.convert(cellType) }
    }
  }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTrellisConvertedRasterSource(uri, layerId, cellType, bandCount, strategy, options)

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource = {
    GeotrellisReprojectRasterSource(uri, layerId, bandCount, targetCRS, reprojectOptions, strategy, options)
  }

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    GeotrellisResampleRasterSource(uri, layerId, bandCount, resampleGrid, method, strategy, options)
  }
}
