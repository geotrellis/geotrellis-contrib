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

  @transient private lazy val vrt: Dataset = {
    val baseDataset: Dataset = GDAL.open(uri)
    val driver = baseDataset.GetDriver()

    // In order to pass in the command line arguments for Warp in Java,
    // each parameter name and value need to passed as individual strings
    // that follow one another
    val parameters: java.util.Vector[_] =
      new java.util.Vector(
        java.util.Arrays.asList(
          "-s_srs",
          s"${baseDataset.GetProjection}",
          "-t_srs",
          s"${crs.toProj4String}",
          "-r",
          s"${GDAL.deriveResampleMethodString(resampleMethod)}",
          "-et",
          s"$errorThreshold"
        )
      )

    val options = new WarpOptions(parameters)

    val dataset = driver.CreateCopy("reprojected", baseDataset)

    gdal.Warp(dataset, Array(baseDataset), options)

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

  private lazy val reader: GDALReader = GDALReader(vrt)

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val bounds: Map[GridBounds, RasterExtent] =
      windows.map { case targetRasterExtent =>
        val affine: Array[Double] =
          Array[Double](
            targetRasterExtent.extent.xmin,
            rasterExtent.cellwidth,
            geoTransform(2),
            targetRasterExtent.extent.ymax,
            geoTransform(4),
            -rasterExtent.cellheight
          )

        // Need to create 4 seperate arrays to hold each of the the points
        val (colMin, rowMin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
        val (colMax, rowMax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

        // rowMin and rowMax are switched, so the GeoTransformed
        // rowMin of the targetRasterExtent becomes the rowMax of the
        // computed Extent and vice-versa
        gdal.ApplyGeoTransform(
          affine,
          targetRasterExtent.gridBounds.colMax,
          targetRasterExtent.gridBounds.rowMin,
          colMax,
          rowMax
        )

        gdal.ApplyGeoTransform(
          affine,
          targetRasterExtent.gridBounds.colMin,
          targetRasterExtent.gridBounds.rowMax,
          colMin,
          rowMin
        )

        val ex = Extent(colMin.head, rowMin.head, colMax.head, rowMax.head)
        val targetGridBounds = rasterExtent.gridBoundsFor(ex)

        (targetGridBounds, targetRasterExtent)
      }.toMap

    bounds.map { case (targetBounds, re) =>
      val initialTile = reader.read(targetBounds)

      val tile =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val updatedTiles = initialTile.bands.map { band =>
            val protoTile = band.prototype(re.cols, re.rows)

            val colOffset = re.cols - targetBounds.width
            val rowOffset = re.rows - targetBounds.height
            //println(s"\nCols of the initialTile: ${initialTile.cols}")
            //println(s"Rows of the initialTile: ${initialTile.rows}")

            //println(s"\nThis is the col offset: ${colOffset}")
            //println(s"This is the row offset: ${rowOffset}\n")

            protoTile.update(colOffset, rowOffset, band)
            protoTile
          }

          MultibandTile(updatedTiles)
        } else
          initialTile

        Raster(tile, re.extent)
    }.toIterator
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}
