package geotrellis.contrib.vlm.gdal


import geotrellis.raster._

import org.gdal.gdal.Dataset

import spire.syntax.cfor._


abstract class GDALReader(dataset: Dataset, dataType: GDALDataType) {
  protected val bandCount = dataset.getRasterCount()

  protected lazy val noDataValue: Option[Double] = {
    val arr = Array.ofDim[java.lang.Double](1)
    dataset.GetRasterBand(1).GetNoDataValue(arr)

    Option(arr(0))
  }

  def read(gridBounds: GridBounds): MultibandTile
}

object GDALReader {
  def apply(dataset: Dataset, dataType: GDALDataType): GDALReader =
    dataType match {
      case (ByteConstantNoDataCellType | IntConstantNoDataCellType16) => ShortGDALReader(dataset, dataType)
      case (TypeUInt16 | IntConstantNoDataCellType32) => IntGDALReader(dataset, dataType)
      case (TypeUInt32 | FloatConstantNoDataCellType32) => FloatGDALReader(dataset, dataType)
      case (TypeUnknown | FloatConstantNoDataCellType64) => DoubleGDALReader(dataset, dataType)
      case (TypeCInt16 | TypeCInt32 | TypeCFloat32 | TypeCFloat64) =>
        throw new Exception("Complex datatypes are not supported")
    }
}


case class ShortGDALReader(dataset: Dataset, dataType: GDALDataType) extends GDALReader(dataset, dataType) {
  def read(gridBounds: GridBounds): MultibandTile = {
    val pixelCount: Int = gridBounds.width * gridBounds.height

    // This array will hold all of the data for the given region for each band
    val sourceArray: Array[Short] = Array.ofDim[Short](pixelCount * bandCount)

    val bands: Array[Array[Short]] = Array.ofDim[Array[Short]](bandCount)

    dataset.ReadRaster(
      gridBounds.colMin,
      gridBounds.rowMin,
      gridBounds.width,
      gridBounds.height,
      gridBounds.width,
      gridBounds.height,
      dataType.code,
      sourceArray,
      1 to bands.size toArray,
      0,
      0,
      0
    )

    var index = 0

    cfor(0)(_ < sourceArray.size, _ + pixelCount) { offset =>
      val bandArray: Array[Short] = Array.ofDim[Short](pixelCount)

      System.arraycopy(sourceArray, offset, bandArray, 0, bandArray.size)
      bands(index) = bandArray

      index += 1
    }

    def updateTile: ShortArrayTile => ShortArrayTile =
      noDataValue match {
        case Some(nd) =>
          (tile: ShortArrayTile) =>
            cfor(0)(_ < tile.cols, _ + 1) { col =>
              cfor(0)(_ < tile.rows, _ + 1) { row =>
                if (tile.getDouble(col, row) == nd) { tile.set(col, row, NODATA) }
              }
            }

            tile
        case None => (tile: ShortArrayTile) => tile
      }

    val tiles: Array[ShortArrayTile] =
      bands.map { band => updateTile(ShortArrayTile(band, gridBounds.width, gridBounds.height)) }

    MultibandTile(tiles)
  }
}

case class IntGDALReader(dataset: Dataset, dataType: GDALDataType) extends GDALReader(dataset, dataType) {
  def read(gridBounds: GridBounds): MultibandTile = {
    val pixelCount: Int = gridBounds.width * gridBounds.height

    // This array will hold all of the data for the given region for each band
    val sourceArray: Array[Int] = Array.ofDim[Int](pixelCount * bandCount)

    val bands: Array[Array[Int]] = Array.ofDim[Array[Int]](bandCount)

    dataset.ReadRaster(
      gridBounds.colMin,
      gridBounds.rowMin,
      gridBounds.width,
      gridBounds.height,
      gridBounds.width,
      gridBounds.height,
      dataType.code,
      sourceArray,
      1 to bands.size toArray,
      0,
      0,
      0
    )

    var index = 0

    cfor(0)(_ < sourceArray.size, _ + pixelCount) { offset =>
      val bandArray: Array[Int] = Array.ofDim[Int](pixelCount)

      System.arraycopy(sourceArray, offset, bandArray, 0, bandArray.size)
      bands(index) = bandArray

      index += 1
    }

    def updateTile: IntArrayTile => IntArrayTile =
      noDataValue match {
        case Some(nd) =>
          (tile: IntArrayTile) =>
            cfor(0)(_ < tile.cols, _ + 1) { col =>
              cfor(0)(_ < tile.rows, _ + 1) { row =>
                if (tile.getDouble(col, row) == nd) { tile.set(col, row, NODATA) }
              }
            }

            tile
        case None => (tile: IntArrayTile) => tile
      }

    val tiles: Array[IntArrayTile] =
      bands.map { band => updateTile(IntArrayTile(band, gridBounds.width, gridBounds.height)) }

    MultibandTile(tiles)
  }
}

case class FloatGDALReader(dataset: Dataset, dataType: GDALDataType) extends GDALReader(dataset, dataType) {
  def read(gridBounds: GridBounds): MultibandTile = {
    val pixelCount: Int = gridBounds.width * gridBounds.height

    // This array will hold all of the data for the given region for each band
    val sourceArray: Array[Float] = Array.ofDim[Float](pixelCount * bandCount)

    val bands: Array[Array[Float]] = Array.ofDim[Array[Float]](bandCount)

    dataset.ReadRaster(
      gridBounds.colMin,
      gridBounds.rowMin,
      gridBounds.width,
      gridBounds.height,
      gridBounds.width,
      gridBounds.height,
      dataType.code,
      sourceArray,
      1 to bands.size toArray,
      0,
      0,
      0
    )

    var index = 0

    cfor(0)(_ < sourceArray.size, _ + pixelCount) { offset =>
      val bandArray: Array[Float] = Array.ofDim[Float](pixelCount)

      System.arraycopy(sourceArray, offset, bandArray, 0, bandArray.size)
      bands(index) = bandArray

      index += 1
    }

    def updateTile: FloatArrayTile => FloatArrayTile =
      noDataValue match {
        case Some(nd) =>
          (tile: FloatArrayTile) =>
            cfor(0)(_ < tile.cols, _ + 1) { col =>
              cfor(0)(_ < tile.rows, _ + 1) { row =>
                if (tile.getDouble(col, row) == nd) { tile.set(col, row, NODATA) }
              }
            }

            tile
        case None => (tile: FloatArrayTile) => tile
      }

    val tiles: Array[FloatArrayTile] =
      bands.map { band => updateTile(FloatArrayTile(band, gridBounds.width, gridBounds.height)) }

    MultibandTile(tiles)
  }
}

case class DoubleGDALReader(dataset: Dataset, dataType: GDALDataType) extends GDALReader(dataset, dataType) {
  def read(gridBounds: GridBounds): MultibandTile = {
    val pixelCount: Int = gridBounds.width * gridBounds.height

    // This array will hold all of the data for the given region for each band
    val sourceArray: Array[Double] = Array.ofDim[Double](pixelCount * bandCount)

    val bands: Array[Array[Double]] = Array.ofDim[Array[Double]](bandCount)

    dataset.ReadRaster(
      gridBounds.colMin,
      gridBounds.rowMin,
      gridBounds.width,
      gridBounds.height,
      gridBounds.width,
      gridBounds.height,
      dataType.code,
      sourceArray,
      1 to bands.size toArray,
      0,
      0,
      0
    )

    var index = 0

    cfor(0)(_ < sourceArray.size, _ + pixelCount) { offset =>
      val bandArray: Array[Double] = Array.ofDim[Double](pixelCount)

      System.arraycopy(sourceArray, offset, bandArray, 0, bandArray.size)
      bands(index) = bandArray

      index += 1
    }

    def updateTile: DoubleArrayTile => DoubleArrayTile =
      noDataValue match {
        case Some(nd) =>
          (tile: DoubleArrayTile) =>
            cfor(0)(_ < tile.cols, _ + 1) { col =>
              cfor(0)(_ < tile.rows, _ + 1) { row =>
                if (tile.getDouble(col, row) == nd) { tile.set(col, row, NODATA) }
              }
            }

            tile
        case None => (tile: DoubleArrayTile) => tile
      }

    val tiles: Array[DoubleArrayTile] =
      bands.map { band => updateTile(DoubleArrayTile(band, gridBounds.width, gridBounds.height)) }

    MultibandTile(tiles)
  }
}
