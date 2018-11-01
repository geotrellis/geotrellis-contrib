package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
import geotrellis.raster.resample._

import org.gdal.gdal.{Dataset, WarpOptions, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.net.URI

// All of the logic in this file was taken from:
// https://github.com/geotrellis/geotrellis-gdal/blob/master/gdal/src/main/scala/geotrellis/gdal/Gdal.scala

private[gdal] class GDALException(code: Int, msg: String)
  extends RuntimeException(s"GDAL ERROR $code: $msg")

private[gdal] object GDALException {
  def lastError(): GDALException =
    new GDALException(gdal.GetLastErrorNo, gdal.GetLastErrorMsg)
}

object GDAL {
  gdal.AllRegister()

  def deriveGTCellType(datatype: GDALDataType, noDataValue: Option[Double] = None, typeSizeInBits: Option[Int] = None): CellType =
    datatype match {
      case UnknownType | TypeFloat64 =>
        noDataValue match {
          case Some(nd) => DoubleUserDefinedNoDataCellType(nd)
          case _ => DoubleConstantNoDataCellType
        }
      case TypeByte =>
        typeSizeInBits match {
          case Some(bits) if bits == 1 => BitCellType
          case _ =>
            noDataValue match {
              case Some(nd) => ByteUserDefinedNoDataCellType(nd.toByte)
              case _ => ByteCellType
            }
        }
      case TypeUInt16 =>
        noDataValue match {
          case Some(nd) => UShortUserDefinedNoDataCellType(nd.toShort)
          case _ => UShortConstantNoDataCellType
        }
      case TypeInt16 =>
        noDataValue match {
          case Some(nd) => ShortUserDefinedNoDataCellType(nd.toShort)
          case _ => ShortConstantNoDataCellType
        }
      case TypeUInt32 | TypeInt32 =>
        noDataValue match {
          case Some(nd) => IntUserDefinedNoDataCellType(nd.toInt)
          case _ => IntConstantNoDataCellType
        }
      case TypeFloat32 =>
        noDataValue match {
          case Some(nd) => FloatUserDefinedNoDataCellType(nd.toFloat)
          case _ => FloatConstantNoDataCellType
        }
      case TypeCInt16 | TypeCInt32 | TypeCFloat32 | TypeCFloat64 =>
        throw new Exception("Complex datatypes are not supported")
    }

  def deriveGDALResampleMethod(method: ResampleMethod): Int =
    method match {
      case NearestNeighbor => gdalconstConstants.GRA_NearestNeighbour
      case Bilinear => gdalconstConstants.GRA_Bilinear
      case CubicConvolution => gdalconstConstants.GRA_Cubic
      case CubicSpline => gdalconstConstants.GRA_CubicSpline
      case Lanczos => gdalconstConstants.GRA_Lanczos
      case Average => gdalconstConstants.GRA_Average
      case Mode => gdalconstConstants.GRA_Mode
      case _ => throw new Exception(s"Could not find equivalent GDALResampleMethod for: $method")
    }

  def deriveResampleMethodString(method: ResampleMethod): String =
    method match {
      case NearestNeighbor => "near"
      case Bilinear => "bilinear"
      case CubicConvolution => "cubic"
      case CubicSpline => "cubicspline"
      case Lanczos => "lanczos"
      case Average => "average"
      case Mode => "mode"
      case Max => "max"
      case Min => "min"
      case Median => "med"
      case _ => throw new Exception(s"Could not find equivalent GDALResampleMethod for: $method")
    }

  def openURI(uri: URI): Dataset =
    openPath(VSIPath(uri.toString).vsiPath)

  def open(path: String): Dataset =
    if (VSIPath.isVSIFormatted(path))
      openPath(path)
    else
      openPath(VSIPath(path).vsiPath)

  def openPath(path: String): Dataset = {
    val ds = gdal.Open(path, gdalconstConstants.GA_ReadOnly)
    if(ds == null) throw GDALException.lastError()
    ds
  }

  def warp(dest: String, baseDatasets: Array[Dataset], warpOptions: WarpOptions): Dataset =
    try gdal.Warp(dest, baseDatasets, warpOptions) finally baseDatasets.foreach(_.delete)

  def warp(dest: String, baseDataset: Dataset, warpOptions: GDALWarpOptions): Dataset =
    warp(dest, Array(baseDataset), warpOptions.toWarpOptions)

  def fromGDALWarpOptions(uri: String, list: List[GDALWarpOptions]): Dataset =
    list.foldLeft(open(uri)) { warp("", _, _) }
}
