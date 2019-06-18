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

package geotrellis.contrib.vlm.avro

import geotrellis.contrib.vlm.{BetterRasterMatchers, PaddedTile}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteArrayTile, MultibandTile, Tile}
import geotrellis.store.avro.AvroRecordCodec

import org.scalatest._

class TileCodecsSpec extends FunSpec with Matchers with RasterMatchers with BetterRasterMatchers {
  def roundTripCodec[T: AvroRecordCodec](tile: T): T = {
    val codec = AvroRecordCodec[T]
    codec.decode(codec.encode(tile))
  }
  def roundTrip(tile: Tile): Tile = roundTripCodec(tile).toArrayTile
  def roundTrip(tile: MultibandTile): MultibandTile = roundTripCodec(tile).toArrayTile

  it("Should encode PaddedTile"){
    val tile = ByteArrayTile.fill(127,10,15)
    val ptile = PaddedTile(tile, 0, 0, 10, 15)

    assertTilesEqual(roundTrip(ptile), tile.toArrayTile)
  }

  it("Should encode multiband PaddedTile"){
    val tile = MultibandTile(
      ByteArrayTile.fill(127,10,15),
      ByteArrayTile.fill(100,10,15),
      ByteArrayTile.fill(50,10,15)
    )
    val ptile = tile.mapBands((_, tile) => PaddedTile(tile, 0, 0, 10, 15))

    assertTilesEqual(roundTrip(ptile), tile.toArrayTile)
  }
}
