package geotrellis.contrib.vlm.gdal

import org.gdal.gdal.gdal

object GDALDataType {
  val types =
    List(
      UnknownType, TypeByte, TypeUInt16, TypeInt16, TypeUInt32,
      TypeInt32, TypeFloat32, TypeFloat64, TypeCInt16,
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

// https://github.com/OSGeo/gdal/blob/512aa6ae763424904a986613cd2a45569d8dbe5d/gdal/swig/include/gdal.i#L129-L143
case object UnknownType extends GDALDataType(0)
case object TypeByte extends GDALDataType(1)
case object TypeUInt16 extends GDALDataType(2)
case object TypeInt16 extends GDALDataType(3)
case object TypeUInt32 extends GDALDataType(4)
case object TypeInt32 extends GDALDataType(5)
case object TypeFloat32 extends GDALDataType(6)
case object TypeFloat64 extends GDALDataType(7)
case object TypeCInt16 extends GDALDataType(8)
case object TypeCInt32 extends GDALDataType(9)
case object TypeCFloat32 extends GDALDataType(10)
case object TypeCFloat64 extends GDALDataType(11)
