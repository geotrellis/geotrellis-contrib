package geotrellis.contrib.vlm.gdal


import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._

import org.gdal.gdal._
import org.gdal.osr.SpatialReference
import org.gdal.gdalconst.gdalconstConstants

import spire.syntax.cfor._

import java.nio.ByteBuffer
import java.nio.ByteOrder


case class GDALSourceData(
  cols: Int,
  rows: Int,
  crs: CRS,
  geoTransform: Array[Double],
  extent: Extent,
  rasterExtent: RasterExtent,
  noDataValue: Option[Double],
  dataType: Int,
  cellType: CellType,
  bandCount: Int
) {

  private lazy val invTransform = gdal.InvGeoTransform(geoTransform)

  def gridBoundsForExtent(targetExtent: Extent): GridBounds = {
    val (colMin, rowMin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
    val (colMax, rowMax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

    gdal.ApplyGeoTransform(
      invTransform,
      targetExtent.xmin,
      targetExtent.ymax,
      colMin,
      rowMin
    )

    gdal.ApplyGeoTransform(
      invTransform,
      targetExtent.xmax,
      targetExtent.ymin,
      colMax,
      rowMax
    )

    GridBounds(
      colMin.head.toInt,
      rowMin.head.toInt,
      colMax.head.toInt - 1,
      rowMax.head.toInt - 1
    )
  }

  def gridBoundsForExtent(
    targetExtent: Extent,
    gridCols: Int,
    gridRows: Int
  ): GridBounds = {
    val (colMin, rowMin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
    val (colMax, rowMax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

    gdal.ApplyGeoTransform(
      invTransform,
      targetExtent.xmin,
      targetExtent.ymax,
      colMin,
      rowMin
    )

    gdal.ApplyGeoTransform(
      invTransform,
      targetExtent.xmax,
      targetExtent.ymin,
      colMax,
      rowMax
    )

    GridBounds(
      math.min(math.max(colMin.head.toInt, 0), gridCols - 1),
      math.min(math.max(rowMin.head.toInt, 0), gridRows - 1),
      math.min(math.max(colMax.head.toInt, 0), gridCols - 1),
      math.min(math.max(rowMax.head.toInt, 0), gridRows - 1)
    )
  }

  def extentForGridBounds(gridBounds: GridBounds, sourceExtent: Option[Extent] = None): Extent = {
    val (xmin, ymin) = (Array.ofDim[Double](1), Array.ofDim[Double](1))
    val (xmax, ymax) = (Array.ofDim[Double](1), Array.ofDim[Double](1))

    gdal.ApplyGeoTransform(
      geoTransform,
      gridBounds.colMin,
      gridBounds.rowMin,
      xmin,
      ymax
    )

    gdal.ApplyGeoTransform(
      geoTransform,
      gridBounds.colMax,
      gridBounds.rowMax,
      xmax,
      ymin
    )

    sourceExtent match {
      case Some(ex) =>
        Extent(
          math.max(math.min(xmin.head, ex.xmax), ex.xmin),
          math.max(math.min(ymin.head, ex.ymax), ex.ymin),
          math.max(math.min(xmax.head, ex.xmax), ex.xmin),
          math.max(math.min(ymax.head, ex.ymax), ex.ymin)
        )
      case None =>
        Extent(
          xmin.head,
          ymin.head,
          xmax.head,
          ymax.head
        )
    }
  }
}

object GDALSourceData {
  def apply(
    uri: String
  ): GDALSourceData = {
    val dataset = GDAL.open(uri)
    val data = apply(dataset)

    dataset.delete
    data
  }

  def apply(
    dataset: Dataset
  ): GDALSourceData = {
    val baseBand = dataset.GetRasterBand(1)

    val colsLong: Long = dataset.getRasterXSize
    val rowsLong: Long = dataset.getRasterYSize

    val cols: Int = colsLong.toInt
    val rows: Int = rowsLong.toInt

    val geoTransform: Array[Double] = dataset.GetGeoTransform

    val xmin: Double = geoTransform(0)
    val ymin: Double = geoTransform(3) + geoTransform(5) * rows
    val xmax: Double = geoTransform(0) + geoTransform(1) * cols
    val ymax: Double = geoTransform(3)

    val extent = Extent(xmin, ymin, xmax, ymax)

    val rasterExtent =
      RasterExtent(
        extent,
        geoTransform(1),
        math.abs(geoTransform(5)),
        cols,
        rows
      )

    val bandCount: Int = dataset.getRasterCount

    val crs: CRS = {
      val projection: Option[String] = {
        val proj = dataset.GetProjectionRef

        if (proj == null || proj.isEmpty) None
        else Some(proj)
      }

      projection.map { proj =>
        val srs = new SpatialReference(proj)
        CRS.fromString(srs.ExportToProj4())
      }.getOrElse(CRS.fromEpsgCode(4326))
    }

    val noDataValue: Option[Double] = {
      val arr = Array.ofDim[java.lang.Double](1)
      baseBand.GetNoDataValue(arr)

      arr.head match {
        case null => None
        case value => Some(value.doubleValue)
      }
    }

    val dataType: Int =
      baseBand.getDataType()

    val cellType: CellType = GDAL.deriveGTCellType(dataType)

    GDALSourceData(
      cols,
      rows,
      crs,
      geoTransform,
      extent,
      rasterExtent,
      noDataValue,
      dataType,
      cellType,
      bandCount
    )
  }
}
