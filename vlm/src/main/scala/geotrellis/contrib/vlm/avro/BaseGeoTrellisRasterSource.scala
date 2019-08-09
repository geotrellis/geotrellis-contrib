package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.RasterSource
import geotrellis.store.LayerId

trait BaseGeoTrellisRasterSource extends RasterSource {
  def dataPath: GeoTrellisPath
  def name: GeoTrellisPath = dataPath
  def layerId: LayerId
  def bandCount: Int

  def attributes: Map[String, String] = Map(
    "catalogURI" -> dataPath.value,
    "layerName"  -> layerId.name,
    "zoomLevel"  -> layerId.zoom.toString,
    "bandCount"  -> bandCount.toString
  )
  /** GeoTrellis metadata doesn't allow to query a per band metadata by default. */
  def attributesForBand(band: Int): Map[String, String] = Map.empty

  def metadata: GeoTrellisMetadata = GeoTrellisMetadata(name, crs, bandCount, cellType, gridExtent, resolutions, attributes)
}
