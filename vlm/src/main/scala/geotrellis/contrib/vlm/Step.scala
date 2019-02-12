package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster.CellType
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod


trait Step {
  val name: String
  val message: String

  private[vlm] def replacePrevious(step: Step): Boolean
}

case class ReprojectStep(
  sourceCRS: CRS,
  targetCRS: CRS,
  options: Reproject.Options
) extends Step {
  final val name: String = "Reproject"

  final val message: String =
    s"Reprojecting from $sourceCRS to $targetCRS using the ${options.method} resample method"

  private[vlm] def replacePrevious(step: Step): Boolean =
    step match {
      case _: ReprojectStep => true
      case _ => false
    }
}

case class ResampleStep(
  resampleMethod: ResampleMethod,
  resampleGrid: ResampleGrid
) extends Step {
  final val name: String = "Resample"

  final val message: String =
    s"Resampling source to this grid: $resampleGrid using the $resampleMethod resample method"

  private[vlm] def replacePrevious(step: Step): Boolean =
    step match {
      case _: ResampleStep => true
      case _ => false
    }
}

case class ConvertStep(
  sourceCellType: CellType,
  targetCellType: CellType
) extends Step {
  final val name: String = "Convert"

  final val message: String =
    s"Converting from $sourceCellType to the $targetCellType CellType"

  private[vlm] def replacePrevious(step: Step): Boolean =
    step match {
      case _: ConvertStep => true
      case _ => false
    }
}
