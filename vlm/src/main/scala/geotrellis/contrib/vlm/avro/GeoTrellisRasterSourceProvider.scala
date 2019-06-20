package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm._
import geotrellis.spark.LayerId


class GeoTrellisRasterSourceProvider extends RasterSourceProvider {
  def canProcess(path: String): Boolean =
    try {
      GeoTrellisDataPath(path)
      return true
    } catch {
      case _: Throwable => false
    }

    def rasterSource(path: String): GeotrellisRasterSource = {
      val dataPath = GeoTrellisDataPath(path)
      val layerName = dataPath.layerName

      // TODO: Handle the case where there's no zoom level
      val zoomLevel = dataPath.zoomLevel.get
      val layerId = LayerId(layerName, zoomLevel)

      dataPath.bandCount match {
        case Some(bandCount) => new GeotrellisRasterSource(dataPath, layerId, bandCount)
        case None => new GeotrellisRasterSource(dataPath, layerId)
      }
    }
}
