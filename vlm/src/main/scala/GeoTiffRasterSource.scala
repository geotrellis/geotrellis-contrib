package geotrellis.contrib.vlm

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject.{ReprojectRasterExtent, Reproject}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import cats.effect.IO

import java.net.URI
import java.nio.file.Paths

import geotrellis.util.{FileRangeReader, RangeReader, StreamingByteReader}

class GeoTiffRasterSource(val fileURI: String) extends RasterSource {
  // TODO: uri on RasterReader2 should be String, because java.net.URI does not admit all valid S3 URIs

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

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val intersectionWindows: Traversable[GridBounds] =
      windows.map { case targetRasterExtent =>
        rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
      }

    tiff.crop(intersectionWindows.toSeq).map { case (gb, tile) =>
      Raster(tile, rasterExtent.extentFor(gb))
    }
  }

  /*
  def read(
    windows: Traversable[RasterExtent],
    crs: CRS,
    targetCellType: CellType,
    options: Reproject.Options
  ): Iterator[Raster[MultibandTile]] = {
    val transform = Transform(tiff.crs, crs)
    val backTransform = Transform(crs, tiff.crs)
    // we have multiple footprints that may intersect or be out of bounds

    /* TODO: we could save a lot of work  here if we burned directly from segments to tiles in target projection
     * The actual work that we can parallize over is dealing with TIFF segments.
     */
    val intersectingWindows: Map[GridBounds, RasterExtent] =
      windows.flatMap { case targetRasterExtent =>
        val sourceExtent: Extent = ReprojectRasterExtent.reprojectExtent(targetRasterExtent, backTransform)
        // sourceExtent may be covering pixels that we won't actually need. Tragic, but we only read in squares.
        if (sourceExtent.intersects(tiff.extent)) {
          val sourceGridBounds: GridBounds = tiff.rasterExtent.gridBoundsFor(sourceExtent, clamp = true)
          // sourceGridBounds will contribute to this region
          Some((sourceGridBounds, targetRasterExtent))
        } else None
      }.toMap

    tiff.crop(intersectingWindows.keys.toSeq).map { case (gb, tile) =>
      val targetRasterExtent = intersectingWindows(gb)
      val sourceRaster = Raster(tile.convert(targetCellType), tiff.rasterExtent.extentFor(gb))
      sourceRaster.reproject(targetRasterExtent, transform, backTransform, options)
    }
  }
  */
}
