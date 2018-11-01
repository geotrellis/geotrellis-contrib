package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import org.gdal.osr.SpatialReference

package object gdal extends Serializable {
  implicit class SpatialReferenceMethods(val sr: SpatialReference) extends AnyVal {
    def toCRS: CRS = CRS.fromString(sr.ExportToProj4)
  }
}
