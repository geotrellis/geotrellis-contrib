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
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.AutoHigherResolution
import geotrellis.raster.resample._
import geotrellis.raster.testkit._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.util._
import org.scalatest._

import java.net.MalformedURLException

class GDALConvertedRasterSourceSpec extends FunSpec with RasterMatchers with BetterRasterMatchers with GivenWhenThen {
  val url = Resource.path("img/aspect-tiled.tif")
  val uri = s"file://$url"

  val source: GDALRasterSource = GDALRasterSource(uri)

  val expectedRaster: Raster[MultibandTile] =
    GeoTiffReader
      .readMultiband(url, streaming = false)
      .raster

  val targetExtent = expectedRaster.gridBounds//extent

  describe("Converting to a different CellType") {
    /*
    describe("Byte CellType") {
      it("should convert to: ByteConstantNoDataCellType") {
        val actual = source.convert(ByteConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ByteConstantNoDataCellType) }

        println(s"These are the cols and rows of the actual: ${actual.cols}, ${actual.rows}")
        println(s"These are the cols and rows of the expected: ${expected.cols}, ${expected.rows}")

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: ByteUserDefinedNoDataCellType(10)") {
        val actual = source.convert(ByteUserDefinedNoDataCellType(10)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ByteUserDefinedNoDataCellType(10)) }

        //assertEqual(actual, expected)
        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: ByteCellType") {
        val actual = source.convert(ByteCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ByteCellType) }

        //assertEqual(actual, expected)
        assertRastersEqual(actual, expected, 1.0)
      }
    }

    describe("Short CellType") {
      it("should convert to: ShortConstantNoDataCellType") {
        val actual = source.convert(ShortConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ShortConstantNoDataCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: ShortUserDefinedNoDataCellType(-1)") {
        val actual = source.convert(ShortUserDefinedNoDataCellType(-1)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ShortUserDefinedNoDataCellType(-1)) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: ShortCellType") {
        val actual = source.convert(ShortCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(ShortCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }
    }

    describe("UShort CellType") {
      it("should convert to: UShortConstantNoDataCellType") {
        val actual = source.convert(UShortConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(UShortConstantNoDataCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: UShortUserDefinedNoDataCellType(-1)") {
        val actual = source.convert(UShortUserDefinedNoDataCellType(-1)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(UShortUserDefinedNoDataCellType(-1)) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: UShortCellType") {
        val actual = source.convert(UShortCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(UShortCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }
    }
    */

    describe("Int CellType") {
      /*
      it("should convert to: IntConstantNoDataCellType") {
        val actual = source.convert(IntConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(IntConstantNoDataCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: IntUserDefinedNoDataCellType(-100)") {
        val actual = source.convert(IntUserDefinedNoDataCellType(-100)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(IntUserDefinedNoDataCellType(-100)) }

        assertRastersEqual(actual, expected, 1.0)
      }
      */

      it("should convert to: IntCellType") {
        val actual = source.convert(IntCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(IntCellType) }

        actual.cellType should be (IntCellType)
        expected.cellType should be (IntCellType)

        assertRastersEqual(actual, expected, 1.0)
      }
    }

    /*
    describe("Float CellType") {
      it("should convert to: FloatConstantNoDataCellType") {
        val actual = source.convert(FloatConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(FloatConstantNoDataCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: FloatUserDefinedNoDataCellType(0)") {
        val actual = source.convert(FloatUserDefinedNoDataCellType(0)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(FloatUserDefinedNoDataCellType(0)) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: FloatCellType") {
        val actual = source.convert(FloatCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(FloatCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }
    }

    describe("Double CellType") {
      it("should convert to: DoubleConstantNoDataCellType") {
        val actual = source.convert(DoubleConstantNoDataCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(DoubleConstantNoDataCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: DoubleUserDefinedNoDataCellType(1.0)") {
        val actual = source.convert(DoubleUserDefinedNoDataCellType(1.0)).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(DoubleUserDefinedNoDataCellType(1.0)) }

        assertRastersEqual(actual, expected, 1.0)
      }

      it("should convert to: DoubleCellType") {
        val actual = source.convert(DoubleCellType).read(targetExtent).get
        val expected = source.read(targetExtent).get.mapTile { _.convert(DoubleCellType) }

        assertRastersEqual(actual, expected, 1.0)
      }
    }
    */
  }
}
