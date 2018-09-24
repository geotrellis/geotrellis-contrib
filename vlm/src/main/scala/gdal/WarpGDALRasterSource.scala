package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.RasterSource

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import org.gdal.gdal._
import org.gdal.osr.SpatialReference


case class WarpGDALRasterSource(
  uri: String,
  crs: CRS,
  baseSourceData: GDALSourceData,
  targetRasterExtent: Option[RasterExtent] = None,
  resampleMethod: ResampleMethod = NearestNeighbor,
  errorThreshold: Double = 0.125
) extends RasterSource {
  final val warpParameters = {
    val baseParameters =
      new java.util.Vector(
        java.util.Arrays.asList(
          "-of", "VRT",
          "-s_srs", baseSourceData.crs.toProj4String,
          "-t_srs", crs.toProj4String,
          "-r", s"${GDAL.deriveResampleMethodString(resampleMethod)}",
          "-et", s"$errorThreshold"
        )
    )

    targetRasterExtent match {
      case Some(rasterExtent) =>
        baseParameters.add("-tr")
        baseParameters.add(s"${rasterExtent.cellwidth}")
        baseParameters.add(s"${rasterExtent.cellheight}")
        baseParameters
      case None =>
        baseParameters
    }
  }

  @transient private lazy val vrt: Dataset = {
    val baseDataset = GDAL.open(uri)

    val options = new WarpOptions(warpParameters)

    val dataset = gdal.Warp("", Array(baseDataset), options)

    baseDataset.delete
    dataset
  }

  private lazy val data: GDALSourceData = GDALSourceData(vrt)

  lazy val cols: Int = data.cols
  lazy val rows: Int = data.rows

  lazy val extent: Extent = data.extent

  override lazy val rasterExtent =
    targetRasterExtent.getOrElse(data.rasterExtent)

  lazy val cellType: CellType = data.cellType

  lazy val bandCount: Int = data.bandCount

  lazy val noDataValue: Option[Double] = data.noDataValue

  private lazy val reader: GDALReader = GDALReader(vrt, bandCount, noDataValue)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    read(data.rasterExtent.gridBoundsFor(extent, clamp = false), bands)
    //read(data.gridBoundsForExtent(extent), bands)
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val targetExtent = rasterExtent.extentFor(bounds, clamp = false)
    //val targetExtent = data.extentForGridBounds(bounds)

    //println(s"\nThis targetExtent: $targetExtent")
    //println(s"This targetExtent2:  $targetExtent2")
    val actualBounds = rasterExtent.gridBoundsFor(targetExtent, clamp = true)
    //val actualBounds =
      //data.gridBoundsForExtent(
        //targetExtent,
        //bounds.width,
        //bounds.height
      //)

    println(s"\nThis is the bounds: $bounds")
    println(s"width: ${bounds.width}")
    println(s"height: ${bounds.height}")
    println(s"\nThis is the actualBounds: $actualBounds")
    println(s"width: ${actualBounds.width}")
    println(s"height: ${actualBounds.height}")

    val initialTile = reader.read(actualBounds, bands)
    val tile =
      if (initialTile.cols != bounds.width || initialTile.rows != bounds.height) {
        val tiles =
          initialTile.bands.map { band =>
            val protoTile = band.prototype(bounds.width, bounds.height)

            //println(s"\nThis is the col offset: ${bounds2.colMin - actualBounds.colMin}")
            //println(s"This is the row offset: ${bounds.rowMin - actualBounds.rowMin}")
            //println(s"This is the band's size - cols: ${band.cols} rows: ${band.rows}")

            //protoTile.update(bounds.colMin - actualBounds.colMin, bounds.rowMin - actualBounds.rowMin, band)
            protoTile.update(actualBounds.colMin - actualBounds.colMin, bounds.rowMin - actualBounds.rowMin, band)
            protoTile
          }

        MultibandTile(tiles)
      } else
        initialTile

    println(s"\n This is the size of the tile - cols: ${tile.cols} row: ${tile.rows}")

    Some(Raster(tile, rasterExtent.extentFor(bounds)))
  }

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, data, None, resampleMethod)

  def reproject(targetCRS: CRS, resampleMethod: ResampleMethod, rasterExtent: RasterExtent): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, data, Some(rasterExtent))
}


object WarpGDALRasterSource {
  def apply(
    uri: String,
    crs: CRS,
    targetRasterExtent: Option[RasterExtent],
    resampleMethod: ResampleMethod,
    errorThreshold: Double
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, crs, GDALSourceData(uri), targetRasterExtent, resampleMethod, errorThreshold)

  def apply(
    uri: String,
    crs: CRS,
    targetRasterExtent: Option[RasterExtent],
    resampleMethod: ResampleMethod
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, crs, GDALSourceData(uri), targetRasterExtent, resampleMethod)

  def apply(
    uri: String,
    crs: CRS,
    targetRasterExtent: Option[RasterExtent]
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, crs, GDALSourceData(uri), targetRasterExtent)

  def apply(
    uri: String,
    crs: CRS,
    resampleMethod: ResampleMethod
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, crs, GDALSourceData(uri), None, resampleMethod)
}
