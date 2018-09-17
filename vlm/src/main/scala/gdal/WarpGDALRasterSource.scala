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
  private val baseSpatialReference = {
    val baseDataset: Dataset = GDAL.open(uri)

    val spatialReference = new SpatialReference(baseDataset.GetProjection)

    baseDataset.delete
    spatialReference
  }

  private val targetSpatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(crs.toProj4String)
    spatialReference
  }

  // In order to pass in the command line arguments for Warp in Java,
  // each parameter name and value needs to passed as individual strings
  // that follow one another

  // IDEA: What if we make the command line parameters available
  // so that way users can know how to recreate this operations
  // via the command line if they so chose?
  final val warpParametersSet =
    new java.util.Vector(
      java.util.Arrays.asList(
        "-s_srs",
        baseSpatialReference.ExportToProj4,
        "-t_srs",
        targetSpatialReference.ExportToProj4,
        "-r",
        s"${GDAL.deriveResampleMethodString(resampleMethod)}",
        "-et",
        s"$errorThreshold"
      )
    )

  @transient private lazy val vrt: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    val name = s"/vsimem$uri"

    val options = new WarpOptions(warpParametersSet)

    val dataset = gdal.Warp(name, Array(baseDataset), options)

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
  override lazy val rasterExtent =
    RasterExtent(
      extent,
      geoTransform(1),
      math.abs(geoTransform(5)),
      cols,
      rows
    )

  lazy val bandCount: Int = vrt.getRasterCount

  private lazy val datatype: GDALDataType = {
    val band = vrt.GetRasterBand(1)
    band.getDataType()
  }

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  private lazy val reader = GDALReader(vrt)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val combinedRasterExtents = windows.reduce { _ combine _ }

    windows.map { case targetRasterExtent =>
      // The resulting bounds sometimes contains an extra col and/or row,
      // and it's not clear as to why. This needs to be fixed in order
      // to get this working.

      val affine: Array[Double] = {
        val targetExtent = targetRasterExtent.extent

        val xMin: Double = math.floor(targetExtent.xmin / combinedRasterExtents.cellwidth) * combinedRasterExtents.cellwidth
        val yMax: Double = math.ceil(targetExtent.ymax / combinedRasterExtents.cellheight) * combinedRasterExtents.cellheight

        Array(targetExtent.xmin, combinedRasterExtents.cellwidth, geoTransform(2), targetExtent.ymax, geoTransform(4), -combinedRasterExtents.cellheight)
      }

      val adjustedExtent = {
        val (xmin, ymin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
        val (xmax, ymax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

        val gridBounds = targetRasterExtent.gridBounds

        gdal.ApplyGeoTransform(
          affine,
          gridBounds.colMin,
          gridBounds.rowMin,
          xmin,
          ymax
        )

        gdal.ApplyGeoTransform(
          affine,
          gridBounds.colMax,
          gridBounds.rowMax,
          xmax,
          ymin
        )

        Extent(xmin.head, ymin.head, xmax.head, ymax.head)
      }

      //val bounds = rasterExtent.gridBoundsFor(adjustedExtent)
      //val bounds = rasterExtent.createAlignedRasterExtent(adjustedExtent).gridBounds
      //val bounds = RasterExtent(adjustedExtent, targetRasterExtent.cols, targetRasterExtent.rows).gridBounds
      //val bounds = combinedRasterExtents.gridBoundsFor(adjustedExtent)
      val bounds = combinedRasterExtents.createAlignedRasterExtent(adjustedExtent).gridBounds

      println(s"\nThese are the bounds of the targetRasterExtent: ${targetRasterExtent.gridBounds}")
      println(s"These are the computed bounds: ${bounds}")
      println(s"This is the extent of the targetRasterExtent: ${targetRasterExtent.extent}")
      println(s"This is the computed extent: ${adjustedExtent}")
      val tile = reader.read(bounds)

     Raster(tile, targetRasterExtent.extent)
    }.toIterator
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}
