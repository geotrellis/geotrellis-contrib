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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.PaddedTile
import geotrellis.raster.{ArrayMultibandTile, MultibandTile, Tile}
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs.Implicits._

import org.apache.avro._
import org.apache.avro.generic._

import scala.collection.JavaConverters._

trait Implicits extends Serializable {
  implicit def paddedTileCodec: AvroRecordCodec[PaddedTile] = new AvroRecordCodec[PaddedTile] {
    def schema = SchemaBuilder
      .record("PaddedTile").namespace("geotrellis.contrib.vlm")
      .fields()
      .name("chunk").`type`(tileUnionCodec.schema).noDefault()
      .name("colOffset").`type`().intType().noDefault()
      .name("rowOffset").`type`().intType().noDefault()
      .name("cols").`type`().intType().noDefault()
      .name("rows").`type`().intType().noDefault()
      .endRecord()

    def encode(tile: PaddedTile, rec: GenericRecord) = {
      rec.put("chunk", tileUnionCodec.encode(tile.chunk))
      rec.put("colOffset", tile.colOffset)
      rec.put("rowOffset", tile.rowOffset)
      rec.put("cols", tile.cols)
      rec.put("rows", tile.rows)
    }

    def decode(rec: GenericRecord) = {
      val chunk     = tileUnionCodec.decode(rec[GenericRecord]("chunk"))
      val colOffset = rec[Int]("colOffset")
      val rowOffset = rec[Int]("rowOffset")
      val cols      = rec[Int]("cols")
      val rows      = rec[Int]("rows")

      PaddedTile(chunk, colOffset, rowOffset, cols, rows)
    }
  }

  implicit def extendedTileUnionCodec = new AvroUnionCodec[Tile](
    byteArrayTileCodec,
    floatArrayTileCodec,
    doubleArrayTileCodec,
    shortArrayTileCodec,
    intArrayTileCodec,
    bitArrayTileCodec,
    uByteArrayTileCodec,
    uShortArrayTileCodec,
    byteConstantTileCodec,
    floatConstantTileCodec,
    doubleConstantTileCodec,
    shortConstantTileCodec,
    intConstantTileCodec,
    bitConstantTileCodec,
    uByteConstantTileCodec,
    uShortConstantTileCodec,
    paddedTileCodec
  )

  implicit def extendedMultibandTileCodec: AvroRecordCodec[MultibandTile] = new AvroRecordCodec[MultibandTile] {
    def schema = SchemaBuilder
      .record("ArrayMultibandTile").namespace("geotrellis.raster")
      .fields()
      .name("bands").`type`().array().items.`type`(extendedTileUnionCodec.schema).noDefault()
      .endRecord()

    def encode(tile: MultibandTile, rec: GenericRecord) = {
      val bands = for (i <- 0 until tile.bandCount) yield tile.band(i)
      rec.put("bands", bands.map(extendedTileUnionCodec.encode).asJavaCollection)
    }

    def decode(rec: GenericRecord) = {
      val bands = rec.get("bands")
        .asInstanceOf[java.util.Collection[GenericRecord]]
        .asScala // notice that Avro does not have native support for Short primitive
        .map(extendedTileUnionCodec.decode)
        .toArray

      ArrayMultibandTile(bands)
    }
  }
}

object Implicits extends Implicits
