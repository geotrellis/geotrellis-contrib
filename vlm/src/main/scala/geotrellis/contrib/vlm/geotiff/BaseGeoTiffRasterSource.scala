package geotrellis.contrib.vlm.geotiff

import geotrellis.contrib.vlm.RasterSource
import geotrellis.raster.CellType
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, Tags}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.util.RangeReader

trait BaseGeoTiffRasterSource extends RasterSource {
  val dataPath: GeoTiffPath
  def name: GeoTiffPath = dataPath

  protected val baseTiff: Option[MultibandGeoTiff]

  @transient lazy val tiff: MultibandGeoTiff =
    baseTiff.getOrElse(GeoTiffReader.readMultiband(RangeReader(dataPath.value), streaming = true))

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)
  def tags: Tags = tiff.tags
  def metadata: GeoTiffMetadata = GeoTiffMetadata(name, crs, bandCount, cellType, gridExtent, resolutions, tags)

  /** Returns the GeoTiff head tags. */
  def attributes: Map[String, String] = tags.headTags
  /** Returns the GeoTiff per band tags. */
  def attributesForBand(band: Int): Map[String, String] = tags.bandTags.lift(band).getOrElse(Map.empty)
}
