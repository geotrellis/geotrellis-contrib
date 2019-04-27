/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import com.azavea.gdal.GDALWarp

import java.net.MalformedURLException

trait GDALBaseRasterSource extends RasterSource {
  val vsiPath: String = if (VSIPath.isVSIFormatted(uri)) uri else try {
    VSIPath(uri).vsiPath
  } catch {
    case _: Throwable =>
      throw new MalformedURLException(
          s"Invalid URI passed into the GDALRasterSource constructor: ${uri}." +
          s"Check geotrellis.contrib.vlm.gdal.VSIPath constrains, " +
          s"or pass VSI formatted String into the GDALRasterSource constructor manually."
      )
  }

  /** options to override some values on transformation steps, should be used carefully as these params can change the behaviour significantly */
  val options: GDALWarpOptions

  lazy val datasetType: Int = options.datasetType

  // current dataset
  @transient lazy val dataset: GDALDataset = {
    val gdalPath =
      if (VSIPath.isVSIFormatted(uri)) uri
      else VSIPath(uri).vsiPath

    GDALDataset(gdalPath, options.toWarpOptionsList.toArray)
  }

  lazy val bandCount: Int = dataset.bandCount

  lazy val crs: CRS = dataset.crs

  // noDataValue from the previous step
  lazy val noDataValue: Option[Double] = dataset.noDataValue(GDALWarp.SOURCE)

  lazy val dataType: Int = dataset.dataType

  lazy val cellType: CellType = dstCellType.getOrElse(dataset.cellType)

  lazy val gridExtent: GridExtent[Long] = dataset.rasterExtent(datasetType).toGridType[Long]

  /** Resolutions of available overviews in GDAL Dataset
    *
    * These resolutions could represent actual overview as seen in source file
    * or overviews of VRT that was created as result of resample operations.
    */
  lazy val resolutions: List[GridExtent[Long]] = dataset.resolutions(datasetType).map(_.toGridType[Long])

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    bounds
      .toIterator
      .flatMap { gb => gridBounds.intersection(gb) }
      .map { gb =>
        val tile = dataset.readMultibandTile(gb.toGridType[Int], bands.map(_ + 1), datasetType)
        val extent = this.gridExtent.extentFor(gb)
        convertRaster(Raster(tile, extent))
      }
  }

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALRasterSource(uri, options.reproject(gridExtent, crs, targetCRS, reprojectOptions))

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALRasterSource(uri, options.resample(gridExtent, resampleGrid))

  /** Converts the contents of the GDALRasterSource to the [[TargetCellType]].
   *
   *  Note:
   *
   *  GDAL handles Byte data differently than GeoTrellis. Unlike GeoTrellis,
   *  GDAL treats all Byte data as Unsigned Bytes. Thus, the output from
   *  converting to a Signed Byte CellType can result in unexpected results.
   *  When given values to convert to Byte, GDAL takes the following steps:
   *
   *  1. Checks to see if the values falls in [0, 255].
   *  2. If the value falls outside of that range, it'll clamp it so that
   *  it falls within it. For example: -1 would become 0 and 275 would turn
   *  into 255.
   *  3. If the value falls within that range and is a floating point, then
   *  GDAL will round it up. For example: 122.492 would become 122 and 64.1
   *  would become 64.
   *
   *  Thus, it is recommended that one avoids converting to Byte without first
   *  ensuring that no data will be lost.
   *
   *  Note:
   *
   *  It is not currently possible to convet to the [[BitCellType]] using GDAL.
   *  @group convert
   */
  def convert(targetCellType: TargetCellType): RasterSource = {
    /** To avoid incorrect warp cellSize transformation, we need explicitly set dimensions. */
    val convertOptions =
      GDALBaseRasterSource
        .createConvertOptions(targetCellType, noDataValue)
        .map(_.copy(dimensions = Some(cols.toInt -> rows.toInt)))
        .toList

    val targetOptions = (convertOptions :+ options).reduce { _ combine _ }
    GDALRasterSource(uri, targetOptions, Some(targetCellType))
  }

  def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(this.gridBounds)), bands)
    if (it.hasNext) Some(it.next) else None
  }

  override def readExtents(extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] = {
    // TODO: clamp = true when we have PaddedTile ?
    val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = false))
    readBounds(bounds, 0 until bandCount)
  }
}

object GDALBaseRasterSource {
  def createConvertOptions(targetCellType: TargetCellType, noDataValue: Option[Double]): Option[GDALWarpOptions] =
    targetCellType match {
      case ConvertTargetCellType(target) =>
        target match {
          case BitCellType => throw new Exception("Cannot convert GDALRasterSource to the BitCellType")

          case ByteConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List(Byte.MinValue.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case ByteCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List("None"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case ByteUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case UByteConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List(0.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case UByteCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List("none"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case UByteUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Byte"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case ShortConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Int16"),
              dstNoData = List(Short.MinValue.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case ShortCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Int16"),
              dstNoData = List("None"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case ShortUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Int16"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case UShortConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("UInt16"),
              dstNoData = List(0.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case UShortCellType =>
            Option(GDALWarpOptions(
              outputType = Some("UInt16"),
              dstNoData = List("None"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case UShortUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("UInt16"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case IntConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Int32"),
              dstNoData = List(Int.MinValue.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case IntCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Int32"),
              dstNoData = List("None"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case IntUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Int32"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case FloatConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Float32"),
              dstNoData = List(Float.NaN.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case FloatCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Float32"),
              dstNoData = List("NaN"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case FloatUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Float32"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))

          case DoubleConstantNoDataCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Float64"),
              dstNoData = List(Double.NaN.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case DoubleCellType =>
            Option(GDALWarpOptions(
              outputType = Some("Float64"),
              dstNoData = List("NaN"),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
          case DoubleUserDefinedNoDataCellType(value) =>
            Option(GDALWarpOptions(
              outputType = Some("Float64"),
              dstNoData = List(value.toString),
              srcNoData = noDataValue.map { _.toString }.toList
            ))
        }
      case _ => None
    }
}
