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

import cats.syntax.option._
import org.gdal.gdal.Dataset

case class GDALConvertedRasterSource(
  uri: String,
  targetCellType: CellType,
  strategy: OverviewStrategy = AutoHigherResolution,
  options: GDALWarpOptions = GDALWarpOptions(),
  baseWarpList: List[GDALWarpOptions] = Nil,
  @transient parentDatasets: Array[Dataset] = Array()
) extends GDALBaseRasterSource {
  def resampleMethod = options.resampleMethod

  private val exceptionMessage = s"Cannot convert to $targetCellType using GDAL"

  override lazy val cellType: CellType = targetCellType

  lazy val warpOptions: GDALWarpOptions = {
    val res =
      targetCellType match {
        case BitCellType => throw new Exception(exceptionMessage)

        case ByteConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Byte"),
            dstNoData = List(Byte.MinValue)
          )
        case ByteCellType =>
          GDALWarpOptions(outputType = Some("Byte"), dstNoData = List())
        case ByteUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(outputType = Some("Byte"), dstNoData = List(value))

        case UByteConstantNoDataCellType =>
          throw new Exception(exceptionMessage)
        case UByteCellType =>
          throw new Exception(exceptionMessage)
        case UByteUserDefinedNoDataCellType(v) =>
          throw new Exception(exceptionMessage)

        case ShortConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List(Short.MinValue),
            srcNoData = noDataValue.toList
          )
        case ShortCellType =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List(),
            srcNoData = noDataValue.toList
          )
        case ShortUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Int16"),
            dstNoData = List(value),
            srcNoData = noDataValue.toList
          )

        case UShortConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List(0),
            srcNoData = noDataValue.toList
          )
        case UShortCellType =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List(),
            srcNoData = noDataValue.toList
          )
        case UShortUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("UInt16"),
            dstNoData = List(value),
            srcNoData = noDataValue.toList
          )

        case IntConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List(Int.MinValue),
            srcNoData = noDataValue.toList
          )
        case IntCellType =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List(),
            srcNoData = noDataValue.toList
          )
        case IntUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Int32"),
            dstNoData = List(value),
            srcNoData = noDataValue.toList
          )

        case FloatConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List(Float.NaN),
            srcNoData = noDataValue.toList
          )
        case FloatCellType =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List(),
            srcNoData = noDataValue.toList
          )
        case FloatUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Float32"),
            dstNoData = List(value),
            srcNoData = noDataValue.toList
          )

        case DoubleConstantNoDataCellType =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List(Double.NaN),
            srcNoData = noDataValue.toList
          )
        case DoubleCellType =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List(),
            srcNoData = noDataValue.toList
          )
        case DoubleUserDefinedNoDataCellType(value) =>
          GDALWarpOptions(
            outputType = Some("Float64"),
            dstNoData = List(value),
            srcNoData = noDataValue.toList
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

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = rasterExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  override def convert(cellType: CellType, strategy: OverviewStrategy): RasterSource =
    GDALConvertedRasterSource(uri, cellType, strategy, options, warpList)

  override def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): RasterSource =
    GDALReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, options, warpList)

  override def resample(resampleGrid: ResampleGrid, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    GDALResampleRasterSource(uri, resampleGrid, method, strategy, options, warpList)
}
