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

package geotrellis.contrib.vlm.gdal.effect

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal.{DefaultDomain, GDALPath, GDALDataset, GDALMetadata, GDALMetadataDomain, GDALWarpOptions}
import geotrellis.contrib.vlm.gdal.GDALDataset.DatasetType
import geotrellis.contrib.vlm.effect._
import geotrellis.contrib.vlm.effect.geotiff.UnsafeLift
import geotrellis.contrib.vlm.avro._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._

import cats._
import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._

case class GDALRasterSource[F[_]: Monad: UnsafeLift](
                                                      dataPath: GDALPath,
                                                      options: F[GDALWarpOptions] = GDALWarpOptions.EMPTY,
                                                      private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSourceF[F] {
  def name: GDALPath = dataPath
  val path: String = dataPath.value

  lazy val datasetType: F[DatasetType] = options.map(_.datasetType)

  // current dataset
  @transient lazy val datasetF: F[GDALDataset] =
    options >>= { opt => UnsafeLift[F].apply { GDALDataset(path, opt.toWarpOptionsList.toArray) } }

  lazy val bandCount: F[Int] = datasetF.map(_.bandCount)

  lazy val crs: F[CRS] = datasetF.map(_.crs)

  // noDataValue from the previous step
  lazy val noDataValue: F[Option[Double]] = datasetF.map(_.noDataValue(GDALDataset.SOURCE))

  lazy val dataType: F[Int] = datasetF.map(_.dataType)

  lazy val cellType: F[CellType] = dstCellType.fold(datasetF.map(_.cellType))(Monad[F].pure)

  lazy val gridExtent: F[GridExtent[Long]] =
    (datasetF, datasetType).mapN { case (dataset, datasetType) =>
      dataset.rasterExtent(datasetType).toGridType[Long]
    }

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: F[List[GridExtent[Long]]] =
    (datasetF, datasetType).mapN { case (dataset, datasetType) => dataset.resolutions(datasetType).map(_.toGridType[Long]) }

  /**
    * Fetches a default metadata from the default domain.
    * If there is a need in some custom domain, use the metadataForDomain function.
    */
  lazy val metadata: F[GDALMetadata] = GDALMetadata(this, datasetF, DefaultDomain :: Nil)

  /**
    * Fetches a metadata from the specified [[GDALMetadataDomain]] list.
    */
  def metadataForDomain(domainList: List[GDALMetadataDomain]): F[GDALMetadata] = GDALMetadata(this, datasetF, domainList)

  /**
    * Fetches a metadata from all domains.
    */
  def metadataForAllDomains: F[GDALMetadata] = GDALMetadata(this, datasetF)

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    UnsafeLift[F].apply {
      (datasetF, gridBounds, gridExtent, datasetType).mapN { (dataset, gridBounds, gridExtent, datasetType) =>
        bounds
          .toIterator
          .flatMap { gb => gridBounds.intersection(gb) }
          .map { gb =>
            val tile = dataset.readMultibandTile(gb.toGridType[Int], bands.map(_ + 1), datasetType)
            val extent = gridExtent.extentFor(gb)
            convertRaster(Raster(tile, extent))
          }
      }
    } flatten
  }

  def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): RasterSourceF[F] =
    GDALRasterSource(
      dataPath,
      (options, gridExtent, crs).mapN { (options, gridExtent, crs) =>
        options.reproject(gridExtent, crs, targetCRS, resampleGrid, method)
      }
    )

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSourceF[F] =
    GDALRasterSource(
      dataPath,
      (options, gridExtent).mapN { (options, gridExtent) => options.resample(gridExtent, resampleGrid) }
    )

  /** Converts the contents of the GDALRasterSource to the [[TargetCellType]].
   *
   *  Note:
   *
   *  GDAL handles Byte data differently than GeoTrellis. Unlike GeoTrellis,
   *  GDAL treats all Byte data as Unsigned Bytes. Thus, the output from
   *  converting to a Signed Byte CellType can result in unexpected results.
   *  When given values to convert to Byte, GDAL takes the following steps:
   *
   *  1. Checks to see if the values falls in [0, 255].
   *  2. If the value falls outside of that range, it'll clamp it so that
   *  it falls within it. For example: -1 would become 0 and 275 would turn
   *  into 255.
   *  3. If the value falls within that range and is a floating point, then
   *  GDAL will round it up. For example: 122.492 would become 122 and 64.1
   *  would become 64.
   *
   *  Thus, it is recommended that one avoids converting to Byte without first
   *  ensuring that no data will be lost.
   *
   *  Note:
   *
   *  It is not currently possible to convert to the [[BitCellType]] using GDAL.
   *  @group convert
   */
  def convert(targetCellType: TargetCellType): RasterSourceF[F] = {
    /** To avoid incorrect warp cellSize transformation, we need explicitly set target dimensions. */
    GDALRasterSource(
      dataPath,
      (options, cols, rows, noDataValue).mapN { (options, cols, rows, noDataValue) =>
        options.convert(targetCellType, noDataValue, Some(cols.toInt -> rows.toInt))
      },
      Some(targetCellType)
    )
  }

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
    val bounds: F[GridBounds[Long]] = (gridExtent, cellSize).mapN { (gridExtent, cellSize) => gridExtent.gridBoundsFor(extent.buffer(- cellSize.width / 2, - cellSize.height / 2), clamp = false) }
    bounds >>= (read(_, bands))
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] = {
    gridBounds >>= { gridBounds => readBounds(List(bounds).flatMap(_.intersection(gridBounds)), bands) >>= (it => UnsafeLift[F].apply { it.next }) }
  }

  override def readExtents(extents: Traversable[Extent]): F[Iterator[Raster[MultibandTile]]] = {
    val bounds = gridExtent.map(gridExtent => extents.map(gridExtent.gridBoundsFor(_, clamp = false)))
    (bounds, bandCount).mapN { (bounds, bandCount) => readBounds(bounds, 0 until bandCount) } flatten
  }
}
