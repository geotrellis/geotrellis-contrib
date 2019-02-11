/*
 * Copyright 2018 Azavea
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
import geotrellis.gdal._
import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{AutoHigherResolution, OverviewStrategy}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject

import org.gdal.gdal.Dataset

import cats.syntax.option._


case class GDALConvertedRasterSource(
  uri: String,
  targetCellType: CellType,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[gdal] val options: GDALWarpOptions = GDALWarpOptions(),
  private[gdal] val baseWarpList: List[GDALWarpOptions] = Nil,
  @transient private[gdal] val parentDatasets: Array[Dataset] = Array()
) extends GDALBaseRasterSource {
  def resampleMethod = options.resampleMethod

  override lazy val cellType: CellType = targetCellType

  lazy private[gdal] val warpOptions: GDALWarpOptions = {
    val res =
      targetCellType match {
        case BitCellType => throw new Exception("Cannot convert GDALRasterSource to the BitCellType")

        case ByteConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List(Byte.MinValue.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case ByteCellType =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List("None"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case ByteUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case UByteConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List(0.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case UByteCellType =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List("none"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case UByteUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case ShortConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List(Short.MinValue.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case ShortCellType =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List("None"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case ShortUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case UShortConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List(0.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case UShortCellType =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List("None"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case UShortUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case IntConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List(Int.MinValue.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case IntCellType =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List("None"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case IntUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case FloatConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List(Float.NaN.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case FloatCellType =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List("NaN"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case FloatUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )

        case DoubleConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List(Double.NaN.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case DoubleCellType =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List("NaN"),
            srcNoData = noDataValue.map { _.toString }.toList
          )
        case DoubleUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List(value.toString),
            srcNoData = noDataValue.map { _.toString }.toList
          )
      }

    options combine res
  }

  override def read(bounds: GridBounds, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val it = readBounds(List(bounds).flatMap(_.intersection(this)), bands)
    if (it.hasNext) {
      val raster = it.next

      Some(
        raster.mapTile { _.convert(targetCellType) }
      )
    } else
      None
  }

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GDALConvertedRasterSource(uri, cellType, strategy, options, warpList)

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options, warpList)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options, warpList)
}
