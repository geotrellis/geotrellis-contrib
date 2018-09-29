package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._
import org.gdal.gdal.{Dataset, WarpOptions, gdal}
import org.gdal.osr.SpatialReference

import scala.collection.JavaConverters._

case class GDALResampleRasterSource(
  uri: String,
  resampleGrid: ResampleGrid,
  method: ResampleMethod = NearestNeighbor
) extends RasterSource {
  @transient private lazy val vrt: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    val rasterExtent: RasterExtent = {
      val colsLong: Long = baseDataset.getRasterXSize
      val rowsLong: Long = baseDataset.getRasterYSize

      val cols: Int = colsLong.toInt
      val rows: Int = rowsLong.toInt

      val geoTransform: Array[Double] = baseDataset.GetGeoTransform

      val xmin: Double = geoTransform(0)
      val ymin: Double = geoTransform(3) + geoTransform(5) * rows
      val xmax: Double = geoTransform(0) + geoTransform(1) * cols
      val ymax: Double = geoTransform(3)

      RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
    }

    val targetRasterExtent = resampleGrid(rasterExtent)

    val warpParams =
      List(
        "-of", "VRT",
        "-tr", targetRasterExtent.cellwidth.toString, targetRasterExtent.cellheight.toString,
        "-r", s"${GDAL.deriveResampleMethodString(method)}"
      ).asJava

    val woptions = new WarpOptions(new java.util.Vector(warpParams))
    val dataset = gdal.Warp("", Array(baseDataset), woptions)
    baseDataset.delete
    dataset
  }

  lazy val bandCount: Int = vrt.getRasterCount

  lazy val crs: CRS = {
    val projection: Option[String] = {
      val proj = vrt.GetProjectionRef

      if (proj == null || proj.isEmpty) None
      else Some(proj)
    }

    projection.map { proj =>
      val srs = new SpatialReference(proj)
      CRS.fromString(srs.ExportToProj4())
    }.getOrElse(CRS.fromEpsgCode(4326))
  }

  private lazy val geoTransform: Array[Double] = vrt.GetGeoTransform
  private lazy val invGeoTransofrm: Array[Double] = gdal.InvGeoTransform(geoTransform)

  private lazy val reader: GDALReader = GDALReader(vrt)

  lazy val cellType: CellType = {
    val (noDataValue, bufferType, typeSizeInBits) = {
      val baseBand = vrt.GetRasterBand(1)

      val arr = Array.ofDim[java.lang.Double](1)
      baseBand.GetNoDataValue(arr)

      val nd = arr.headOption.flatMap(Option(_)).map(_.doubleValue())
      val bufferType = baseBand.getDataType
      val typeSizeInBits = gdal.GetDataTypeSize(bufferType)
      (nd, bufferType, Some(typeSizeInBits))
    }
    GDAL.deriveGTCellType(bufferType, noDataValue, typeSizeInBits)
  }

  def rasterExtent: RasterExtent = {
    val colsLong: Long = vrt.getRasterXSize
    val rowsLong: Long = vrt.getRasterYSize

    val cols: Int = colsLong.toInt
    val rows: Int = rowsLong.toInt

    val xmin: Double = geoTransform(0)
    val ymin: Double = geoTransform(3) + geoTransform(5) * rows
    val xmax: Double = geoTransform(0) + geoTransform(1) * cols
    val ymax: Double = geoTransform(3)

    RasterExtent(Extent(xmin, ymin, xmax, ymax), cols, rows)
  }

  override def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val tuples =
      bounds.map { gb =>
        val re = rasterExtent.rasterExtentFor(gb)
        val boundsClamped = rasterExtent.gridBoundsFor(re.extent, clamp = true)
        val bounds = rasterExtent.gridBoundsFor(re.extent, clamp = false)
        (bounds, boundsClamped, re)
      }

    tuples.map { case (gb, gbc, re) =>
      val initialTile = reader.read(gbc, bands = bands)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val updatedTiles = initialTile.bands.map { band =>
            // TODO: it can't be larger than the source is, fix it
            val protoTile = band.prototype(math.min(re.cols, cols), math.min(re.rows, rows))

            protoTile.update(gb.colMin - gbc.colMin, gb.rowMin - gbc.rowMin, band)
            protoTile
          }

          (gb, MultibandTile(updatedTiles))
        } else (gbc, initialTile)

      Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
  }

  def reproject(targetCRS: CRS, options: Reproject.Options): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, options)

  def resample(resampleGrid: ResampleGrid, method: ResampleMethod): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method)

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(this)), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = true when we have PaddedTile ?
    val bounds = extents.map(rasterExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, 0 until bandCount)
  }
}
