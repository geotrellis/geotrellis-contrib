package geotrellis.contrib.vlm.effect.geotiff

import geotrellis.contrib.vlm.effect.RasterSourceF
import geotrellis.contrib.vlm.geotiff.{GeoTiffMetadata, GeoTiffPath}
import geotrellis.raster.CellType
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, Tags}
import geotrellis.util.RangeReader

import cats.Monad
import cats.syntax.functor._
import cats.syntax.apply._

abstract class BaseGeoTiffRasterSource[F[_]: Monad: UnsafeLift] extends RasterSourceF[F] {
  val dataPath: GeoTiffPath
  def name: GeoTiffPath = dataPath

  // memoize tiff, not useful only in a local fs case
  @transient lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(RangeReader(dataPath.value), streaming = true)
  @transient lazy val tiffF: F[MultibandGeoTiff] = UnsafeLift[F].apply(tiff)

  def bandCount: F[Int] = tiffF.map(_.bandCount)
  def cellType: F[CellType] = dstCellType.fold(tiffF.map(_.cellType))(Monad[F].pure)
  def tags: F[Tags] = tiffF.map(_.tags)
  def metadata: F[GeoTiffMetadata] = (Monad[F].pure(name), crs, bandCount, cellType, gridExtent, resolutions, tags).mapN(GeoTiffMetadata)

  /** Returns the GeoTiff head tags. */
  def attributes: F[Map[String, String]] = tags.map(_.headTags)
  /** Returns the GeoTiff per band tags. */
  def attributesForBand(band: Int): F[Map[String, String]] = tags.map(_.bandTags.lift(band).getOrElse(Map.empty))
}
