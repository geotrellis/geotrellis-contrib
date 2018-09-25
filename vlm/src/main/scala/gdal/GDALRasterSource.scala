package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vector._
import org.gdal.osr.SpatialReference


case class GDALRasterSource(uri: String) extends RasterSource {
  @transient private lazy val dataset = GDAL.open(uri)


  private lazy val geoTransform: Array[Double] = dataset.GetGeoTransform

  // lazy val extent = Extent(xmin, ymin, xmax, ymax)

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

  def rasterExtent: RasterExtent = {
    val colsLong: Long = dataset.getRasterXSize
    val rowsLong: Long = dataset.getRasterYSize

    val cols: Int = colsLong.toInt
    val rows: Int = rowsLong.toInt

    val xmin: Double = geoTransform(0)
    val ymin: Double = geoTransform(3) + geoTransform(5) * rows
    val xmax: Double = geoTransform(0) + geoTransform(1) * cols
    val ymax: Double = geoTransform(3)

    val re = RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
    re
  }

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource = null

  /** Reads a window for the extent.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    *
    * @group read
    */
  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = None

  /** Reads a window for pixel bounds.
    * Return extent may be smaller than requested extent around raster edges.
    * May return None if the requested extent does not overlap the raster extent.
    *
    * @group read
    */
  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = None

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val tuples =
      bounds.map { gb =>
        val re = rasterExtent.rasterExtentFor(gb)
        val boundsClamped = rasterExtent.gridBoundsFor(re.extent, clamp = true)
        val bounds = rasterExtent.gridBoundsFor(re.extent, clamp = false)
        (bounds, boundsClamped, re)
      }

    tuples.map { case (gb, gbc, re) =>
      val initialTile = reader.read(gbc)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val updatedTiles = initialTile.bands.map { band =>
            val protoTile = band.prototype(re.cols, re.rows)

            protoTile.update(gb.colMin - gbc.colMin, gb.rowMin - gbc.rowMin, band)
            protoTile
          }

          (gb, MultibandTile(updatedTiles))
        } else (gbc, initialTile)

      Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
  }

  def reproject(crs: CRS, options: Reproject.Options): WarpGDALRasterSourceV2 = {
    WarpGDALRasterSourceV2(uri, crs, options)
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = true when we have PaddedTile
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, Nil)
  }

  /*def withCRS2(targetCRS: CRS, layout: LayoutDefinition, resampleMethod: ResampleMethod = NearestNeighbor): WarpGDALRasterSourceV2 =
    WarpGDALRasterSourceV2(uri, targetCRS, layout, resampleMethod)*/
}
