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
import com.typesafe.scalalogging.LazyLogging


/** RasterSource that resamples on read from underlying GeoTrellis layer.
 *
 * Note:
 * The constructor is unfriendly.
 * This class is not intended to constructed directly by the users.
 * Refer to [[GeoTrellisRasterSource]] for example of correct setup.
 * It is expected that the caller has significant pre-computed information  about the layers.
 *
 * @param attributeStore the source of metadata for the layers, used for reading
 * @param dataPath dataPath of the GeoTrellis catalog that can format a given path to be read in by a AttributeStore
 * @param layerId The specific layer we're sampling from
 * @param sourceLayers list of layers we can can sample from for futher resample
 * @param gridExtent the desired pixel grid for the layer
 * @param resampleGrid Target pixel grid to which the layer will be resampled
 * @param resampleMethod Resampling method used when fitting data to target grid
 */
class GeotrellisResampleRasterSource(
  val attributeStore: AttributeStore,
  val dataPath: GeoTrellisDataPath,
  val layerId: LayerId,
  val sourceLayers: Stream[Layer],
  val gridExtent: GridExtent[Long],
  val resampleMethod: ResampleMethod = NearestNeighbor,
  val targetCellType: Option[TargetCellType] = None
) extends RasterSource with LazyLogging { self =>

  lazy val reader = CollectionLayerReader(attributeStore, dataPath.path)

  /** Source layer metadata  that needs to be resampled */
  lazy val sourceLayer: Layer = sourceLayers.find(_.id == layerId).get

  /** GridExtent of source pixels that needs to be resampled */
  lazy val sourceGridExtent: GridExtent[Long] = sourceLayer.gridExtent

  def crs: CRS = sourceLayer.metadata.crs

  def cellType: CellType = dstCellType.getOrElse(sourceLayer.metadata.cellType)

  def bandCount: Int = sourceLayer.bandCount

  lazy val resolutions: List[GridExtent[Long]] = sourceLayers.map(_.gridExtent).toList

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val tileBounds = sourceLayer.metadata.mapTransform.extentToBounds(extent)
    def msg = s"\u001b[32mread($extent)\u001b[0m = ${dataPath.toString} ${sourceLayer.id} ${sourceLayer.metadata.cellSize} @ ${sourceLayer.metadata.crs} TO $cellSize -- reading ${tileBounds.size} tiles"
    if (tileBounds.size < 1024) // Assuming 256x256 tiles this would be a very large request
      logger.debug(msg)
    else
      logger.warn(msg + " (large read)")

    GeotrellisRasterSource.readIntersecting(reader, layerId, sourceLayer.metadata, extent, bands)
      .map { raster =>
        val targetRasterExtent = gridExtent.createAlignedRasterExtent(extent)
        logger.trace(s"\u001b[31mTargetRasterExtent\u001b[0m: ${targetRasterExtent} ${targetRasterExtent.dimensions}")
        raster.resample(targetRasterExtent, resampleMethod)
      }
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds
      .intersection(this.gridBounds)
      .map(gridExtent.extentFor(_).buffer(- cellSize.width / 2, - cellSize.height / 2))
      .flatMap(read(_, bands))
  }

  def reprojection(targetCRS: CRS, resampleGrid: Option[ResampleGrid[Long]] = None, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): GeotrellisReprojectRasterSource = {
    val (closestLayerId, gridExtent) = GeotrellisReprojectRasterSource.getClosestSourceLayer(targetCRS, sourceLayers, null /*reprojectOptions*/, strategy)
    new GeotrellisReprojectRasterSource(attributeStore, uri, layerId, sourceLayers, gridExtent, targetCRS, null /*reprojectOptions*/, targetCellType)
  }
  /** Resample underlying RasterSource to new grid extent
   * Note: ResampleGrid will be applied to GridExtent of the source layer, not the GridExtent of this RasterSource
   */
  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeotrellisResampleRasterSource = {
    val resampledGridExtent = resampleGrid(this.gridExtent)
    val closestLayer = GeotrellisRasterSource.getClosestResolution(sourceLayers.toSeq, resampledGridExtent.cellSize, strategy)(_.metadata.layout.cellSize).get
    // TODO: if closestLayer is w/in some marging of desired CellSize, return GeoTrellisRasterSource instead
    new GeotrellisResampleRasterSource(attributeStore, dataPath, closestLayer.id, sourceLayers, resampledGridExtent, method, targetCellType)
  }

  def convert(targetCellType: TargetCellType): GeotrellisResampleRasterSource = {
    new GeotrellisResampleRasterSource(attributeStore, dataPath, layerId, sourceLayers, gridExtent, resampleMethod, Some(targetCellType))
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    extents.toIterator.flatMap(read(_, bands))

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    bounds.toIterator.flatMap(_.intersection(this.gridBounds).flatMap(read(_, bands)))

  override def toString: String =
    s"GeoTrellisResampleRasterSource(${dataPath.toString},$layerId,$gridExtent,$resampleMethod)"
}
