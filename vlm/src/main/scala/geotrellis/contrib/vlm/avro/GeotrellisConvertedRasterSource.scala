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
  baseLayerId: LayerId,
  cellType: CellType,
  bandCount: Int,
  strategy: OverviewStrategy = AutoHigherResolution
) extends RasterSource { self =>

  lazy val reader = CollectionLayerReader(uri)
  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](baseLayerId)
  lazy val rasterExtent: RasterExtent =
    metadata.layout.createAlignedGridExtent(metadata.extent).toRasterExtent()

  lazy val resolutions: List[RasterExtent] = GeotrellisRasterSource.getResolutions(reader, baseLayerId.name)

  def crs: CRS = metadata.crs
  def resampleMethod: Option[ResampleMethod] = None

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val result = GeotrellisRasterSource.read(reader, baseLayerId, metadata, extent, bands)

    result.map { raster =>
      raster.mapTile { _.convert(cellType) }
    }
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTrellisConvertedRasterSource(uri, baseLayerId, cellType, bandCount, strategy)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeotrellisReprojectRasterSource = {
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, targetCRS, reprojectOptions, strategy)
  }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    GeotrellisResampleRasterSource(uri, baseLayerId, bandCount, resampleGrid, method, strategy)
  }
}
