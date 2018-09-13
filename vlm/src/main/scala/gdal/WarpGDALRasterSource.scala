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
  resampleMethod: ResampleMethod = NearestNeighbor,
  errorThreshold: Double = 0.125
) extends RasterSource {
  private lazy val spatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(crs.toProj4String)
    spatialReference
  }

  // In order to pass in the command line arguments for Translate in Java,
  // each parameter name and value need to passed as individual strings
  // that follow one another

  // IDEA: What if we make the command line parameters available
  // so that way users can know how to recreate this operations
  // via the command line if they so chose.
  final val baseTranslateParameters = {
    val baseDataset: Dataset = GDAL.open(uri)

    val params =
      new java.util.Vector(
        java.util.Arrays.asList(
          "-r",
          s"${GDAL.deriveResampleMethodString(resampleMethod)}",
          "-projwin_srs",
          crs.toProj4String
        )
      )

    baseDataset.delete
    params
  }

  @transient private lazy val vrt: Dataset = {

    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    // TODO: change how the dataset is created so that
    // it takes the direction of the picture into account.
    val dataset =
      gdal.AutoCreateWarpedVRT(
        baseDataset,
        null,
        spatialReference.ExportToWkt,
        GDAL.deriveGDALResampleMethod(resampleMethod),
        errorThreshold
      )

    baseDataset.delete

    dataset
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

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val baseDataset = GDAL.open(uri)

    val combinedRasterExtents: RasterExtent = windows.reduce { _ combine _ }

    val targetExtent = combinedRasterExtents.extent
    val name = s"/vsimem/$uri/${targetExtent}"

    val updatedTranslateParameters = {
      // TODO: Find a better way of doing this.
      val updated = baseTranslateParameters
      updated.add("-tr")
      updated.add(s"${combinedRasterExtents.cellwidth} ${combinedRasterExtents.cellheight}")
      updated.add("-projwin")
      updated.add(s"${targetExtent.xmin} ${targetExtent.ymin} ${targetExtent.xmax} ${targetExtent.ymax}")

      val result = updated

      // A RunTimeError is thrown if these aren't removed.
      // I'm not sure why.
      updated.removeElement("-tr")
      updated.removeElement(s"${combinedRasterExtents.cellwidth} ${combinedRasterExtents.cellheight}")
      updated.removeElement("-projwin")
      updated.removeElement(s"${targetExtent.xmin} ${targetExtent.ymin} ${targetExtent.xmax} ${targetExtent.ymax}")

      result
    }

    val options = new TranslateOptions(updatedTranslateParameters)

    val targetDataset = gdal.Translate(name, baseDataset, options)

    val reader = GDALReader(targetDataset)

    val tiles =
      windows.map { case targetRasterExtent =>
        val bounds = combinedRasterExtents.gridBoundsFor(targetRasterExtent.extent)
        val tile = reader.read(bounds)

        Raster(tile, targetRasterExtent.extent)
      }.toIterator

    targetDataset.delete
    baseDataset.delete
    tiles
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}
