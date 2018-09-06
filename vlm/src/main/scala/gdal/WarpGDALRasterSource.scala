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
  resampleMethod: GDALResampleMethod = GDALNearestNeighbor,
  errorThreshold: Double = 0.125
) extends RasterSource {
  private lazy val spatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(crs.toProj4String)
    spatialReference
  }

  @transient private lazy val vrt: Dataset = {
    val dataset: Dataset = GDAL.open(uri)

    val ds = gdal.AutoCreateWarpedVRT(dataset, null, spatialReference.ExportToWkt(), resampleMethod, errorThreshold)
    dataset.delete()
    ds
  }

  private lazy val colsLong: Long = vrt.getRasterXSize
  private lazy val rowsLong: Long = vrt.getRasterYSize

  require(
    colsLong * rowsLong <= Int.MaxValue,
    s"Cannot read this raster, cols * rows is greater than the maximum array index: ${colsLong * rowsLong}"
  )

  def cols: Int = colsLong.toInt
  def rows: Int = rowsLong.toInt

  private lazy val geoTransform: Array[Double] = vrt.GetGeoTransform

  private lazy val xmin: Double = geoTransform(0)
  private lazy val ymin: Double = geoTransform(3) + geoTransform(5) * rows
  private lazy val xmax: Double = geoTransform(0) + geoTransform(1) * cols
  private lazy val ymax: Double = geoTransform(3)

  lazy val extent = Extent(xmin, ymin, xmax, ymax)
  override lazy val rasterExtent = RasterExtent(extent, cols, rows)

  lazy val bandCount: Int = vrt.getRasterCount

  private lazy val datatype: GDALDataType = {
    val band = vrt.GetRasterBand(1)
    band.getDataType()
  }

  private lazy val reader: GDALReader = GDALReader(vrt, datatype)

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val bounds: Map[GridBounds, RasterExtent] =
      windows.flatMap { case targetRasterExtent =>
        val existingRegion = rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
        //(existingRegion, targetRasterExtent)

        if (existingRegion.width > targetRasterExtent.cols || existingRegion.height > targetRasterExtent.rows)
          existingRegion.split(targetRasterExtent.cols, targetRasterExtent.rows).map { gb =>
            (gb, targetRasterExtent)
          }
        else
          Seq((existingRegion, targetRasterExtent))

        /*
        existingRegion.intersection(targetRasterExtent.gridBounds) match {
          case Some(intersection) =>
            //Some((existingRegion, targetRasterExtent))
            Some((intersection, targetRasterExtent))
          case None => None
        }
        */
      }.toMap

    bounds.map { case (gb, re) =>
      val initialTile = reader.read(gb)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          println(s"initialTile: cols = ${initialTile.cols} rows = ${initialTile.rows}")
          println(s"rasterExtent: cols = ${re.cols} rows = ${re.rows}\n")

          val targetBounds = rasterExtent.gridBoundsFor(re.extent, clamp = false)

          val updatedTiles = initialTile.bands.map { band =>
            val protoTile = band.prototype(re.cols, re.rows)
            println(s"colMin: ${gb.colMin}, rowMin: ${gb.rowMin}")
            println(s"256 - colMin: ${256 - gb.colMin}, 256 - rowMin: ${256 - gb.rowMin}\n")

            //protoTile.update(re.cols - gb.colMin, re.rows - gb.rowMin, band)
            protoTile.update(re.cols - gb.width, re.rows - gb.height, band)
            protoTile
          }

          (targetBounds, MultibandTile(updatedTiles))
        } else
          (gb, initialTile)

        //Raster(tile, rasterExtent.extentFor(gridBounds))
        Raster(tile, re.extent)
    }.toIterator
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}
