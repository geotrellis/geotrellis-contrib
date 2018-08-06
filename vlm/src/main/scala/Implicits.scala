package geotrellis.contrib.vlm


import geotrellis.raster.GridBounds
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.vector.Geometry

import java.net.URI


object Implicits extends Implicits


trait Implicits {
  implicit def stringToURI(str: String): URI =
    new URI(str)

  implicit class WithGeoTiffInfoMethods(val info: GeoTiffInfo) {
    def getSegmentLayoutTransform: GeoTiffSegmentLayoutTransform =
      GeoTiffSegmentLayoutTransform(info.segmentLayout, info.bandCount)

    private[geotrellis] def windowsByPartition(
      maxSize: Int,
      partitionBytes: Long,
      geometry: Option[Geometry]
    ): Array[Array[GridBounds]] = {
      val windows =
        geometry match {
          case Some(geometry) =>
            info.segmentLayout.listWindows(maxSize, info.extent, geometry)
          case None =>
            info.segmentLayout.listWindows(maxSize)
        }

      info
        .segmentLayout
        .partitionWindowsBySegments(
          windows,
          partitionBytes / math.max(info.cellType.bytes, 1)
        )
    }
  }
}
