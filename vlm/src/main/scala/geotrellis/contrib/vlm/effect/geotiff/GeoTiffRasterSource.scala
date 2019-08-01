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

package geotrellis.contrib.vlm.effect.geotiff

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.effect._
import geotrellis.contrib.vlm.geotiff.{GeoTiffDataPath, GeoTiffMetadata}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.util.RangeReader

import cats._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.instances.list._

case class GeoTiffRasterSource[F[_]: Monad: UnsafeLift](
  dataPath: GeoTiffDataPath,
  private[vlm] val targetCellType: Option[TargetCellType] = None
) extends RasterSourceF[F] {
  // memoize tiff, not useful only in a local fs case
  @transient lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(RangeReader(dataPath.path), streaming = true)
  @transient lazy val tiffF: F[MultibandGeoTiff] = UnsafeLift[F].apply(tiff)

  lazy val gridExtent: F[GridExtent[Long]] = tiffF.map(_.rasterExtent.toGridType[Long])
  lazy val resolutions: F[List[GridExtent[Long]]] = tiffF.map { tiff =>
    tiff.rasterExtent.toGridType[Long] :: tiff.overviews.map(_.rasterExtent.toGridType[Long])
  }

  def crs: F[CRS] = tiffF.map(_.crs)
  def bandCount: F[Int] = tiffF.map(_.bandCount)
  def cellType: F[CellType] = dstCellType.fold(tiffF.map(_.cellType))(Monad[F].pure)
  def metadata: F[GeoTiffMetadata] = GeoTiffMetadata(this, tiffF.map(_.tags))

  def reprojection(targetCRS: CRS, resampleGrid: ResampleGrid[Long] = IdentityResampleGrid, method: ResampleMethod = NearestNeighbor, strategy: OverviewStrategy = AutoHigherResolution): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(dataPath, targetCRS, resampleGrid, method, strategy, targetCellType = targetCellType)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(dataPath, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffRasterSource[F] =
    GeoTiffRasterSource(dataPath, Some(targetCellType))

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] =
    (tiffF, gridExtent).mapN { (tiff, gridExtent) =>
      val bounds = gridExtent.gridBoundsFor(extent, clamp = false).toGridType[Int]
      val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
      UnsafeLift[F].apply {
        val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
          // TODO: shouldn't GridExtent give me Extent for types other than N ?
          Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = false))
        }

        tiff.synchronized {
          if (it.isEmpty) throw new Exception("The requested extent has no intersections with the actual RasterSource")
          else convertRaster(it.next)
        }
      }
    }.flatten

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    readBounds(List(bounds), bands) >>= (iter => tiffF.map { _.synchronized(iter.next) })

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    val bounds: F[List[GridBounds[Long]]] = extents.toList.traverse { e => gridExtent.map(_.gridBoundsFor(e, clamp = true)) }
    bounds >>= (readBounds(_, bands))
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    (tiffF, gridBounds, gridExtent).mapN { (tiff, gridBounds, gridExtent) =>
      val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
      val intersectingBounds: Seq[GridBounds[Int]] =
        bounds.flatMap(_.intersection(gridBounds)).toSeq.map(_.toGridType[Int])

      UnsafeLift[F].apply {
        geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
          convertRaster(Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = true)))
        }
      }
    }.flatten
}

