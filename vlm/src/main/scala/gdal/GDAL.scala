package geotrellis.contrib.vlm.gdal

import org.gdal.gdal.gdal
import org.gdal.gdal.Dataset
import org.gdal.gdalconst.gdalconstConstants


// All of the logic in this file was taken from:
// https://github.com/geotrellis/geotrellis-gdal/blob/master/gdal/src/main/scala/geotrellis/gdal/Gdal.scala

class GDALException(code: Int, msg: String)
    extends RuntimeException(s"GDAL ERROR $code: $msg")

object GDALException {
  def lastError(): GDALException =
    new GDALException(gdal.GetLastErrorNo, gdal.GetLastErrorMsg)
}

object GDAL {
  gdal.AllRegister()

  def open(path: String): Dataset = {
    val ds = gdal.Open(path, gdalconstConstants.GA_ReadOnly)
    if(ds == null) {
      throw GDALException.lastError()
    }
    ds
  }
}
