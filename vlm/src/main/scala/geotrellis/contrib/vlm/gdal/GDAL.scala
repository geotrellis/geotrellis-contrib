package geotrellis.contrib.vlm.gdal

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.util.LazyLogging

import cats.syntax.foldable._
import cats.syntax.option._
import cats.instances.list._
import cats.instances.either._
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import java.net.URI

// All of the logic in this file was taken from:
// https://github.com/geotrellis/geotrellis-gdal/blob/master/gdal/src/main/scala/geotrellis/gdal/Gdal.scala

private [gdal] class GDALException(code: Int, msg: String)
  extends RuntimeException(s"GDAL ERROR $code: $msg")

private [gdal] object GDALException {
  def lastError(): GDALException =
    new GDALException(gdal.GetLastErrorNo, gdal.GetLastErrorMsg)
}

object GDAL extends LazyLogging {
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

  @transient lazy val cache: Cache[String, GDALDataset] =
    Scaffeine()
      .maximumSize(1000)
      .removalListener[String, GDALDataset] { case (_, dataset, _) =>
        logger.info(s"removalListener: ${dataset}")
        if(dataset != null) dataset.close
      }
      .weakValues()
      .build[String, GDALDataset]

  /** We may want to force invalidate caches, in case we don't trust GC too much */
  def cacheCleanUp: Unit = cache.invalidateAll()

  def openPath(path: String): Dataset = {
    lazy val getDS = GDALDataset(gdal.Open(path, gdalconstConstants.GA_ReadOnly))
    val ds = cache.getIfPresent(path.base64).getOrElse {
      logger.info(s"could not open Dataset, creating new one: ${path}")
      cache.put(path, getDS)
      getDS
    }.underlying
    if(ds == null) throw GDALException.lastError()
    ds
  }

  // parentWarpOptions is a tuple of a path to the initial dataset and a list of previous transformations
  // it is required to calculate a proper cache key
  def warp(dest: String, baseDatasets: Array[Dataset], warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): Dataset = {
    lazy val getDS = GDALDataset(gdal.Warp(dest, baseDatasets, warpOptions.toWarpOptions))
    val key = s"${parentWarpOptions.name}${warpOptions.name}".base64
    val ds = cache.getIfPresent(key).getOrElse {
      logger.info(s"could not open WARPDataset, creating new one: ${parentWarpOptions.name}${warpOptions.name}")
      cache.put(key, getDS)
      getDS
    }.underlying
    if(ds == null) throw GDALException.lastError()
    ds
  }

  def warp(dest: String, baseDataset: Dataset, warpOptions: GDALWarpOptions, parentWarpOptions: Option[(String, List[GDALWarpOptions])]): Dataset =
    warp(dest, Array(baseDataset), warpOptions, parentWarpOptions)

  def fromGDALWarpOptions(uri: String, list: List[GDALWarpOptions]): Dataset = {
    // if we want to perform warp operations
    if (list.nonEmpty) {
      // let's find the latest cached dataset, once we'll find smth let's stop
      val Left(Some(dataset)) =
        list.zipWithIndex.reverse.foldLeftM(Option.empty[Dataset]) { case (acc, (_, idx)) =>
          acc match {
            // successful dataset retrive, in case for some reason there is smth non empty
            case ds @ Some(_) =>  Left(ds)

            // we haven't read anything
            case None =>
              if (idx == 0) {
                Left(Option(list.zipWithIndex.foldLeft(open(uri)) { case (ds, (ops, index)) =>
                  warp("", ds, ops, (uri, list.take(index)).some)
                }))
              } else {
                val result = cache.getIfPresent((uri, list.take(idx)).name).map { c => list.drop(idx).foldLeft(c.underlying) { warp("", _, _, (uri, list.drop(idx)).some) } }
                if (result.isEmpty) Right(result)
                else Left(result)
              }
          }
        }

      dataset
    } else open(uri) // just open a GDAL dataset
  }
}
