/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.gdal

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample._

object GDALUtils {
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

  def dataTypeToCellType(datatype: GDALDataType, noDataValue: Option[Double] = None, typeSizeInBits: Option[Int] = None): CellType =
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
        throw new UnsupportedOperationException("Complex datatypes are not supported")
    }

  def deriveOverviewStrategyString(strategy: OverviewStrategy): String = strategy match {
    case Auto(n) => s"AUTO-$n"
    case AutoHigherResolution => "AUTO"
    case Base => "NONE"
  }
}
