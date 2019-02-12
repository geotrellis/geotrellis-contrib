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
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata}
import geotrellis.spark.io._

case class GeotrellisResampleRasterSource(
  uri: String,
  baseLayerId: LayerId,
  bandCount: Int,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[avro] val parentOptions: RasterViewOptions = RasterViewOptions()
) extends GeotrellisBaseRasterSource {
  lazy val rasterExtent: RasterExtent =
    parentOptions.rasterExtent match {
      case Some(re) => resampleGrid(re)
      case None => resampleGrid(baseRasterExtent)
    }

  def crs: CRS = parentCRS
  def cellType: CellType = parentCellType

  @transient lazy val layerId: LayerId =
    GeotrellisRasterSource.getClosestLayer(resolutions, layerIds, baseLayerId, rasterExtent.cellSize, strategy)

  def resampleMethod: Option[ResampleMethod] = Some(method)

  private[avro] val options: RasterViewOptions =
    parentOptions.copy(
      rasterExtent = Some(rasterExtent),
      readMethod = Some(read)
    )

  lazy val layerName = baseLayerId.name
  lazy val resolutions: List[RasterExtent] = GeotrellisRasterSource.getResolutions(reader, layerName)
  lazy val layerIds: Seq[LayerId] = GeotrellisRasterSource.getLayerIdsByName(reader, layerName)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    GeotrellisRasterSource.readIntersecting(reader, layerId, metadata, extent, bands)
      .map { raster =>
        val targetRasterExtent = rasterExtent.createAlignedRasterExtent(extent)
        raster.resample(targetRasterExtent, method).mapTile { _.convert(cellType) }
      }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent: Extent = metadata.extentFor(bounds)
    read(extent, bands)
  }

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeotrellisReprojectRasterSource =
    GeotrellisReprojectRasterSource(uri, baseLayerId, bandCount, targetCRS, reprojectOptions, strategy, options)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GeotrellisResampleRasterSource(uri, baseLayerId, bandCount, resampleGrid, method, strategy, options)

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTrellisConvertedRasterSource(uri, baseLayerId, cellType, bandCount, strategy, options)

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(extent => read(extent, bands))

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(bounds => read(bounds, bands))
}
