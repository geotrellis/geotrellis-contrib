package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, RasterRegionReproject, Reproject}
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import cats.effect.IO

import java.net.URI
import java.nio.file.Paths

import geotrellis.util.{FileRangeReader, RangeReader, StreamingByteReader}

class GeoTiffRasterSource(val fileURI: String) extends RasterSource {

  private def getByteReader(): StreamingByteReader = {
    val javaURI = new URI(fileURI)
    val rr =  javaURI.getScheme match {
      case "file" | null =>
        FileRangeReader(Paths.get(javaURI).toFile)

      case "s3" => ???
      //     val s3Uri = new AmazonS3URI(java.net.URLDecoder.decode(uri, "UTF-8"))
      //     val s3Client = new AmazonS3Client(new AWSAmazonS3Client(new DefaultAWSCredentialsProviderChain))
      //     S3RangeReader(s3Uri.getBucket, s3Uri.getKey, s3Client)

      case scheme =>
        throw new IllegalArgumentException(s"Unable to read scheme $scheme at $uri")
    }
    new StreamingByteReader(rr)
  }

  @transient private lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(getByteReader, streaming = true)

  def uri: URI = new URI(fileURI)

  def extent: Extent = tiff.extent
  def crs: CRS = tiff.crs
  def cols: Int = tiff.tile.cols
  def rows: Int = tiff.tile.rows
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = tiff.cellType

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): GeoTiffRasterSource =
    new GeoTiffRasterSource(fileURI) {
      @transient private lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(getByteReader, streaming = true)

      private val baseCols: Int = tiff.cols
      private val baseRows: Int = tiff.rows
      private val baseCRS: CRS = tiff.crs
      private val baseExtent: Extent = tiff.extent
      private val baseRasterExtent: RasterExtent = RasterExtent(baseExtent, baseCols, baseRows)

      override def bandCount: Int = tiff.bandCount
      override def cellType: CellType = tiff.cellType

      override def crs: CRS = targetCRS

      private val transform: Transform = Transform(baseCRS, targetCRS)
      private val backTransform: Transform = Transform(targetCRS, baseCRS)

      override lazy val rasterExtent: RasterExtent = ReprojectRasterExtent(baseRasterExtent, transform)
      override def extent: Extent = rasterExtent.extent

      override def cols: Int = rasterExtent.cols
      override def rows: Int = rasterExtent.rows

      override def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
        val intersectingWindows: Map[GridBounds, RasterExtent] =
          windows.map { case targetRasterExtent =>
            val sourceExtent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
            val sourceGridBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = false)

            (sourceGridBounds, targetRasterExtent)
          }.toMap

        tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
          val targetRasterExtent = intersectingWindows(gb)
          val sourceRaster = Raster(tile, baseRasterExtent.extentFor(gb, clamp = false))

          val rr = implicitly[RasterRegionReproject[MultibandTile]]
          rr.regionReproject(
            sourceRaster,
            baseCRS,
            targetCRS,
            targetRasterExtent,
            targetRasterExtent.extent.toPolygon,
            resampleMethod
          )
        }
      }
    }

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val intersectionWindows: Traversable[GridBounds] =
      windows.map { case targetRasterExtent =>
        rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = false)
      }

    tiff.crop(intersectionWindows.toSeq).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb, clamp = false))
    }
  }
}

object GeoTiffRasterSource {
  def apply(fileURI: String): GeoTiffRasterSource =
    new GeoTiffRasterSource(fileURI)

  def apply(
    fileURI: String,
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): GeoTiffRasterSource =
    apply(fileURI).withCRS(targetCRS, resampleMethod)
}
