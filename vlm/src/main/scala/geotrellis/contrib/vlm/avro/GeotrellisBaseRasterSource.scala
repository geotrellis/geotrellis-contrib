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


trait GeotrellisBaseRasterSource extends RasterSource {
  val uri: String
  val baseLayerId: LayerId
  val layerId: LayerId
  val bandCount: Int

  private[avro] val parentOptions: RasterViewOptions
  private[avro] val options: RasterViewOptions

  lazy val reader = CollectionLayerReader(uri)

  lazy val metadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)

  lazy val baseMetadata = reader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](baseLayerId)
  lazy val baseRasterExtent: RasterExtent =
    baseMetadata.layout.createAlignedGridExtent(baseMetadata.extent).toRasterExtent()

  def baseResolutions: List[RasterExtent] = GeotrellisRasterSource.getResolutions(reader, baseLayerId.name)

  def baseCRS: CRS = baseMetadata.crs
  def baseCellType: CellType = baseMetadata.cellType

  protected lazy val parentRasterExtent: RasterExtent =
    parentOptions.rasterExtent.getOrElse(metadata.layout.createAlignedGridExtent(metadata.extent).toRasterExtent())

  def parentCRS: CRS = parentOptions.crs.getOrElse(metadata.crs)
  def parentCellType: CellType = parentOptions.cellType.getOrElse(metadata.cellType)

  def crs: CRS
  def cellType: CellType
  val rasterExtent: RasterExtent

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]]

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]]

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    extents.toIterator.flatMap(extent => read(extent, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds.toIterator.flatMap(bounds => read(bounds, bands))
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource = {
    GeotrellisReprojectRasterSource(uri, layerId, bandCount, targetCRS, reprojectOptions, strategy, options)
  }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    GeotrellisResampleRasterSource(uri, layerId, bandCount, resampleGrid, method, strategy, options)
  }

  def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTrellisConvertedRasterSource(uri, layerId, cellType, bandCount, strategy, options)
}
