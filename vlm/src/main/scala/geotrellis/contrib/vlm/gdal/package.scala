package geotrellis.contrib.vlm

import geotrellis.raster._
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject.{Options => ReprojectOptions}
import geotrellis.gdal._

import cats.implicits._

package object gdal {
  implicit class GDALWarpOptionsMethodExtension(val self: GDALWarpOptions) {
    def reproject(rasterExtent: RasterExtent, sourceCRS: CRS, targetCRS: CRS, reprojectOptions: ReprojectOptions = ReprojectOptions.DEFAULT): GDALWarpOptions = {
      val re = rasterExtent.reproject(sourceCRS, targetCRS, reprojectOptions)

      self.copy(
        cellSize  = re.cellSize.some,
        targetCRS = targetCRS.some,
        sourceCRS = sourceCRS.some,
        resampleMethod = reprojectOptions.method.some
      )
    }

    def resample(gridExtent: => GridExtent, resampleGrid: ResampleGrid): GDALWarpOptions = {
      val rasterExtent = gridExtent.toRasterExtent
      resampleGrid match {
        case Dimensions(cols, rows) => self.copy(te = None, cellSize = None, dimensions = (cols, rows).some)
        case _ =>
          val targetRasterExtent = resampleGrid(rasterExtent)

          println("~~~~~~~~~~~~~~~~")
          println(s"rasterExtent: ${rasterExtent}")
          println(s"targetRasterExtent: ${targetRasterExtent}")
          println("~~~~~~~~~~~~~~~~")

          self.copy(
            // te       = rasterExtent.extent.some,
            cellSize = targetRasterExtent.cellSize.some
          )
      }
    }
  }

}
