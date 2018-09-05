package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample.{ResampleMethod, NearestNeighbor}
import geotrellis.vector._

import org.gdal.osr.SpatialReference


case class GDALRasterSource(uri: String) extends RasterSource {
  @transient private lazy val dataset = GDAL.open(uri)

  private val colsLong: Long = dataset.getRasterXSize
  private val rowsLong: Long = dataset.getRasterYSize

  require(
    colsLong * rowsLong <= Int.MaxValue,
    s"Cannot read this raster, cols * rows is greater than the maximum array index: ${colsLong * rowsLong}"
  )

  def cols: Int = colsLong.toInt
  def rows: Int = rowsLong.toInt

  private lazy val geoTransform: Array[Double] = dataset.GetGeoTransform

  private lazy val xmin: Double = geoTransform(0)
  private lazy val ymin: Double = geoTransform(3) + geoTransform(5) * rows
  private lazy val xmax: Double = geoTransform(0) + geoTransform(1) * cols
  private lazy val ymax: Double = geoTransform(3)

  lazy val extent = Extent(xmin, ymin, xmax, ymax)

  lazy val bandCount: Int = dataset.getRasterCount

  lazy val crs: CRS = {
    val projection: Option[String] = {
      val proj = dataset.GetProjectionRef

      if (proj == null || proj.isEmpty) None
      else Some(proj)
    }

    projection.map { proj =>
      val srs = new SpatialReference(proj)
      CRS.fromString(srs.ExportToProj4())
    }.getOrElse(CRS.fromEpsgCode(4326))
  }

  private lazy val datatype: GDALDataType =
    dataset.GetRasterBand(1).getDataType()

  private lazy val reader: GDALReader = GDALReader(dataset)

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val bounds: Map[GridBounds, RasterExtent] =
      windows.map { case targetRasterExtent =>
        val existingRegion = rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)

        (existingRegion, targetRasterExtent)
      }.toMap

   bounds.map { case (gb, re) =>
     val initialTile = reader.read(gb)

     val (gridBounds, tile) =
       if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
         val targetBounds = rasterExtent.gridBoundsFor(re.extent, clamp = false)

         val updatedTiles = initialTile.bands.map { band =>
           val protoTile = band.prototype(re.cols, re.rows)

           protoTile.update(targetBounds.colMin - gb.colMin, targetBounds.rowMin - gb.rowMin, band)
           protoTile
         }

         (targetBounds, MultibandTile(updatedTiles))
       } else
         (gb, initialTile)

     Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
  }

  def withCRS(targetCRS: CRS, resampleMethod: ResampleMethod = NearestNeighbor): GDALRasterSource = ???
}
