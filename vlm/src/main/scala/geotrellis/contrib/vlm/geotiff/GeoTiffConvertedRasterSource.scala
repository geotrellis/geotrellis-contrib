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

package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

case class GeoTiffConvertedRasterSource(
  uri: String,
  cellType: CellType,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[geotiff] val parentOptions: RasterViewOptions = RasterViewOptions(),
  protected val parentSteps: StepCollection = StepCollection()
) extends GeoTiffBaseRasterSource {
  def crs: CRS = parentCRS
  val rasterExtent: RasterExtent = parentRasterExtent

  protected lazy val currentStep: Option[Step] = Some(ConvertStep(parentCellType, cellType))

  private[geotiff] lazy val options = parentOptions.copy(cellType = Some(cellType))

  lazy val resolutions: List[RasterExtent] = rasterExtent :: baseResolutions

  def resampleMethod: Option[ResampleMethod] = None

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it =
      parentOptions.readExtentsMethod match {
        case Some(readMethod) => readMethod(List(extent), bands)
        case None => readExtents(List(extent), bands)
      }

    if (it.hasNext) {
      val raster = it.next

      Some(
        raster.mapTile { _.convert(cellType) }
      )
    } else
      None
  }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it =
      parentOptions.readBoundsMethod match {
        case Some(readMethod) => readMethod(List(bounds), bands)
        case None => readBounds(List(bounds), bands)
      }

    if (it.hasNext) {
      val raster = it.next

      Some(
        raster.mapTile { _.convert(cellType) }
      )
    } else
      None
  }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    parentOptions.readExtentsMethod match {
      case Some(readMethod) => readMethod(extents, bands).map { _.mapTile { _.convert(cellType) } }
      case None =>
        val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = true))
        readBounds(bounds, bands)
    }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    parentOptions.readBoundsMethod match {
      case Some(readMethod) => readMethod(bounds, bands).map { _.mapTile { _.convert(cellType) } }
      case None =>
        val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
        val intersectingBounds = bounds.flatMap(_.intersection(this)).toSeq
        geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
          Raster(tile.convert(cellType), rasterExtent.extentFor(gb, clamp = true))
        }
    }

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options, stepCollection)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): GeoTiffReprojectRasterSource =
    GeoTiffReprojectRasterSource(
      uri,
      crs,
      Reproject.Options(method = method, targetRasterExtent = Some(resampleGrid(rasterExtent))),
      strategy,
      options,
      stepCollection
    )

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GeoTiffConvertedRasterSource(uri, cellType, strategy, options, stepCollection)
}
