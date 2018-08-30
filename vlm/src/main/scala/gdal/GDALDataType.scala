package geotrellis.contrib.vlm.gdal

import org.gdal.gdal.gdal

object GDALDataType {
  val types =
    List(
      TypeUnknown, ByteConstantNoDataCellType, TypeUInt16, IntConstantNoDataCellType16, TypeUInt32,
      IntConstantNoDataCellType32, FloatConstantNoDataCellType32, FloatConstantNoDataCellType64, TypeCInt16,
      TypeCInt32, TypeCFloat32, TypeCFloat64
    )

  implicit def intToGDALDataType(i: Int): GDALDataType =
    types.find(_.code == i) match {
      case Some(dt) => dt
      case None => sys.error(s"Invalid GDAL data type code: $i")
    }

  implicit def GDALDataTypeToInt(typ: GDALDataType): Int =
    typ.code
}

abstract sealed class GDALDataType(val code: Int) {
  override
  def toString: String = gdal.GetDataTypeName(code)
}

case object TypeUnknown extends GDALDataType(0)
case object ByteConstantNoDataCellType extends GDALDataType(1)
case object TypeUInt16 extends GDALDataType(2)
case object IntConstantNoDataCellType16 extends GDALDataType(3)
case object TypeUInt32 extends GDALDataType(4)
case object IntConstantNoDataCellType32 extends GDALDataType(5)
case object FloatConstantNoDataCellType32 extends GDALDataType(6)
case object FloatConstantNoDataCellType64 extends GDALDataType(7)
case object TypeCInt16 extends GDALDataType(8)
case object TypeCInt32 extends GDALDataType(9)
case object TypeCFloat32 extends GDALDataType(10)
case object TypeCFloat64 extends GDALDataType(11)
