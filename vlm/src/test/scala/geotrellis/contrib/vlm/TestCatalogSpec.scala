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

package geotrellis.contrib.vlm

import TestCatalog._

import org.scalatest.FunSpec
import geotrellis.spark.io.{ValueReader, CollectionLayerReader}
import geotrellis.raster.{Tile, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.io._


import java.io.File


class TestCatalogSpec extends FunSpec with CatalogTestEnvironment {
  val absOutputPath = s"file://${TestCatalog.outputPath}"

  describe("catalog test environment") {
    it("should create catalog before test is run") {
      assert(new File(TestCatalog.outputPath).exists)
    }
  }
  describe("value reader") {
    it("should be able to read test catalog") {
      ValueReader(absOutputPath).reader[SpatialKey, Tile](LayerId("landsat", 0))
    }
    it("should be unable to read non-existent test catalog") {
      assertThrows[AttributeNotFoundError] {
        ValueReader(absOutputPath).reader[SpatialKey, Tile](LayerId("INVALID", 0))
      }
    }
  }
  describe("collection layer reader") {
    it("should be able to read test catalog") {
      CollectionLayerReader(absOutputPath).read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId("landsat", 0))
    }
    it("should be unable to read non-existent test catalog") {
      assertThrows[LayerNotFoundError] {
        CollectionLayerReader(absOutputPath).read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId("INVALID", 0))
      }
    }
  }

}

