package geotrellis.contrib.vlm.gdal


import geotrellis.raster.resample._

object GDALResampleMethod {
  final val methods = List(
    GDALNearestNeighbor,
    GDALBilinear,
    GDALCubic,
    GDALCubicSpline,
    GDALLanczos,
    GDALAverage,
    GDALMode
  )

  implicit def intToGDALResampleMethod(i: Int): GDALResampleMethod =
    methods.find(_.code == i) match {
      case Some(dt) => dt
      case None => sys.error(s"Invalid GDAL resample method code: $i")
    }

  implicit def GDALResampleMethodToInt(method: GDALResampleMethod): Int =
    method.code

  implicit def GTResampleMethodToGDALResampleMethod(method: ResampleMethod): GDALResampleMethod =
    method match {
      case NearestNeighbor => NearestNeighbor
      case Bilinear => GDALBilinear
      case CubicConvolution => GDALCubic
      case CubicSpline => GDALCubicSpline
      case Lanczos => GDALLanczos
      case Average => GDALAverage
      case Mode => GDALMode
      case _ => throw new Exception(s"Could not find equivalent GDALResampleMethod for: $method")
    }
}

abstract sealed class GDALResampleMethod(val code: Int)

case object GDALNearestNeighbor extends GDALResampleMethod(0)
case object GDALBilinear extends GDALResampleMethod(1)
case object GDALCubic extends GDALResampleMethod(2)
case object GDALCubicSpline extends GDALResampleMethod(3)
case object GDALLanczos extends GDALResampleMethod(4)
case object GDALAverage extends GDALResampleMethod(5)
case object GDALMode extends GDALResampleMethod(6)
